from peewee import *
from playhouse.apsw_ext import APSWDatabase
import os
import sys
from tqdm import tqdm
from classes.SampleHash import SampleHash
from prefetch_generator import background


class Inventory(Model):
    file_hash = CharField(index=True)
    file_name = TextField(index=True)
    file_path = TextField()
    file_size = IntegerField()
    is_hashed = BooleanField(default=False)

    class Meta:
        database = None


class InventorySql:
    def __init__(self, database: str = None, path_to_scan: str=None, excludes: list=None) -> None:
        if database is not None:
            Inventory._meta.database = APSWDatabase(database)
        else:
            Inventory._meta.database = APSWDatabase('inventory.db')
        self.db = Inventory._meta.database
        self.db.connect()
        self.scan_path = path_to_scan
        self.file_count = 0
        self.excludes = excludes
        self.bad_paths = [str]
        self.result_items = [Inventory]
        if not Inventory.table_exists():
            self.db.create_tables([Inventory])

    def count_files(self):
        print("Getting files to inventory...")
        for root, dirs, files in os.walk(self.scan_path):
            if root in self.excludes:
                del dirs[:]
                continue
            print("Scanning: {}".format(root))
            self.file_count += len(files)

    def add_inventory_item(self, **kwargs):
        Inventory(**kwargs).save()

    def add_inventory_items(self, l: list):
        with self.db.atomic():
            for idx in range(0, len(l), 100):
                Inventory.insert_many(l[idx:idx+100]).execute()

    def first_pass(self):
        self.count_files()
        data_source = []
        pbar = tqdm(total=self.file_count, ascii=True, desc="Scanning to database: ")
        for root, dirs, files in os.walk(self.scan_path):
            if len(data_source) > 1000:
                self.add_inventory_items(data_source)
                pbar.update(len(data_source))
                data_source.clear()
            if root in self.excludes:
                del dirs[:]
                continue
            for f in files:
                fp = os.path.join(root, f)
                try:
                    data_source.append({
                        'file_hash': '', 'file_name': f, 'file_path': fp, 'file_size':  os.path.getsize(fp)
                    })
                except FileNotFoundError as e:
                    self.bad_paths.append(fp)

        if len(data_source) > 0:
            self.add_inventory_items(data_source)
            pbar.update(len(data_source))
            data_source.clear()
        pbar.close()

        with open("{}_bad_paths.tsv".format(self.db.database), 'w') as fh:
            for p in self.bad_paths:
                fh.write("{}\n".format(p))

    def hash_files(self):
        batch = []
        lim = 1000
        print("Calculating files to Hash:...")
        size_in_db = Inventory.select(fn.SUM(Inventory.file_size)).where(Inventory.is_hashed == False)
        fs = 0
        for r in size_in_db.execute():
            fs = r.file_size

        pbar = tqdm(total=fs, unit='B', ascii=True, unit_scale=True, unit_divisor=1024, desc="Bytes Hashed: ")
        for item in Inventory.select().where(Inventory.is_hashed == False).order_by(Inventory.file_size.desc()):
            if len(batch) > lim:
                self.do_gen(batch, pbar)
                batch.clear()
                batch.append(item)
            else:
                batch.append(item)
        self.do_gen(batch, pbar)
        pbar.close()

    def hash_empties(self):
        batch = []
        lim = 1000
        size_in_db = Inventory.select(fn.SUM(Inventory.file_size)).where(Inventory.file_hash == '')
        fs = 0
        for r in size_in_db.execute():
            fs = r.file_size

        pbar = tqdm(total=fs, unit='B', ascii=True, unit_scale=True, unit_divisor=1024, desc="Bytes Hashed: ")
        for item in Inventory.select().where(Inventory.file_hash == '').order_by(Inventory.file_size.desc()):
            if len(batch) > lim:
                self.do_gen(batch, pbar)
                batch.clear()
                batch.append(item)
            else:
                batch.append(item)
        self.do_gen(batch, pbar)
        pbar.close()

    def do_gen(self, batch: [Inventory], progress: tqdm):
        for inv_item, h, size in self.generator_hash(batch):
            inv_item.file_hash = h
            inv_item.is_hashed = True
            inv_item.save()
            progress.update(size)

    def fix_empties(self):
        for item in Inventory.select().where(Inventory.file_hash == '').order_by(Inventory.file_size.desc()):
            s = item.file_path.split(os.path.sep)
            q = Inventory.delete().where(Inventory.file_path == item.file_path)
            q.execute()

    @background(max_prefetch=8)
    def generator_hash(self, batch: list):
        for inv_item in batch:
            try:
                sh = SampleHash(inv_item.file_path)
                yield inv_item, sh.do_hash(), os.path.getsize(inv_item.file_path)
            except FileNotFoundError as e:
                print(e)
                yield inv_item, '', 0

    def get_duplicate_report(self):
        # TODO: build out this routine
        res = Inventory.raw("SELECT rowid, file_hash, file_size, COUNT(*) as count FROM inventory WHERE file_size != 0 "
                            "group by file_hash "
                            "having COUNT(*) > 1 ORDER BY count DESC")
        total_count = 0
        rep_name = "{}_duplicate_report.tsv".format(self.db.database.replace(".db", ""))
        with open(rep_name, 'w') as fh:
            fh.write("{}\t{}\t{}\n".format("Hash", "Count", "Filename"))
            print("Scanning database for duplicates.")
            for inv in res:
                total_count += inv.count

            pbar = tqdm(total=total_count, ascii=True, desc="Writing Duplicate Report. {}".format(rep_name))
            for inv in res:
                count = Inventory.select().where(Inventory.file_hash == inv.file_hash).count()
                s = "{}\t{}\t{}\n".format(inv.file_hash, count, inv.file_path)
                fh.write(s)
                for item in Inventory.select().where(Inventory.file_hash == inv.file_hash):
                    s = "\t\t{}\n".format(item.file_path)
                    fh.write(s)
                pbar.update(count)
            pbar.close()

    def has_hash(self, hsh: str):
        if Inventory.select().where(Inventory.file_hash == hsh).count() > 0:
            return True
        return False

    def iter_duplicates(self):
        res = Inventory.raw("SELECT rowid, file_hash, file_size, COUNT(*) as count FROM inventory WHERE file_size != 0 "
                            "group by file_hash "
                            "having COUNT(*) > 1 ORDER BY count DESC")
        for inv in res:
            yield inv.file_hash

    def get_example(self, hsh: str) -> []:
        i = []
        for item in Inventory.select().where(Inventory.file_hash == hsh).limit(2):
            i.append(item)
        return i


def get_excludes(fn):
    excludes = []
    with open(fn, 'r') as fh:
        for line in fh.readlines():
            excludes.append(line.strip())
    return excludes

if __name__ == "__main__":
    db_name = sys.argv[1]
    path_to_scan = sys.argv[2]
    pass_num = sys.argv[3]
    exclude = []
    if len(sys.argv) > 4:
        exclude = get_excludes(sys.argv[4])

    isql = InventorySql(db_name, path_to_scan, exclude)
    if pass_num == '1':
        isql.first_pass()
        isql.hash_files()
    if pass_num == '2':
        isql.hash_files()
        #isql.hash_empties()
        #isql.fix_empties()
    if pass_num == '3':
        isql.get_duplicate_report()
