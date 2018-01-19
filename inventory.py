from peewee import *
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
            Inventory._meta.database = SqliteDatabase(database)
        else:
            Inventory._meta.database = SqliteDatabase('inventory.db')
        self.db = Inventory._meta.database
        self.db.connect()
        self.scan_path = path_to_scan
        self.file_count = 0
        self.excludes = excludes
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
                fp = "\\\\?\\{}\\{}".format(root, f)
                data_source.append({
                    'file_hash': '', 'file_name': f, 'file_path': fp, 'file_size': os.path.getsize(fp)
                })
                # print("Added: {}".format(fp))
        pbar.close()

    def hash_files(self):
        batch = []
        lim = 1000
        size_in_db = Inventory.select(fn.SUM(Inventory.file_size)).where(Inventory.is_hashed == False)
        fs = 0
        for r in size_in_db.execute():
            fs = r.file_size

        pbar = tqdm(total=fs, unit='B', ascii=True, unit_scale=True, unit_divisor=1024, desc="Bytes Hashed: ")
        for item in Inventory.select().where(Inventory.is_hashed == False).order_by(Inventory.file_size.desc()):
            if len(batch) < lim:
                batch.append(item)
                continue

            for inv_item, h, size in self.generator_hash(batch):
                inv_item.file_hash = h
                inv_item.is_hashed = True
                inv_item.save()
                pbar.update(size)
            batch = []
        pbar.close()

    @background(max_prefetch=8)
    def generator_hash(self, batch: list):
        for inv_item in batch:
            try:
                sh = SampleHash(inv_item.file_path)
                yield inv_item, sh.do_hash(), os.path.getsize(inv_item.file_path)
            except FileNotFoundError as e:
                yield inv_item, '', 0


    def get_duplicates(self):
        res = Inventory.raw("SELECT rowid, file_hash, COUNT(*) as count FROM main.inventory group by file_hash having COUNT(*) > 1 ORDER BY count DESC")

        for inv in res:
            print()


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
    if pass_num == '2':
        isql.hash_files()
    if pass_num == '3':
        isql.get_duplicates()


