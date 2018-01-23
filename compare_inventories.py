from inventory import InventorySql
from inventory import Inventory
import os


class CompareInventories:

    def __init__(self, first: str, second: str) -> None:
        self.inv1 = first
        self.inv2 = second
        self.second_ex = [Inventory]
        self.first_ex = [Inventory]


    def compare(self):
        dups = []
        first = InventorySql(self.inv1)
        for hsh in first.iter_duplicates():
            dups.append(hsh)
        first.db.close()

        dup = {}
        for hsh in dups:
            #print("Scanning: {}".format(hsh))
            second = InventorySql(self.inv2)
            if second.has_hash(hsh):
                print(hsh)
                dup[hsh] = {}
                dup[hsh][2] = self.get_second_example(hsh)
                second.db.close()
                dup[hsh][1] = self.get_first_example(hsh)
        print()

    def get_first_example(self, hsh) -> [Inventory]:
        a = []
        first = InventorySql(self.inv1)
        for i in first.get_example(hsh):
            a.append(i.file_path)
        first.db.close()
        return a

    def get_second_example(self, hsh) -> [Inventory]:
        a = []
        second = InventorySql(self.inv2)
        for i in second.get_example(hsh):
            a.append(os.path.abspath(i.file_path))
        second.db.close()
        return a



if __name__ == "__main__":
    inv1 = "2018PInventory.db"
    inv2 = "2018_SC_Inventory.db"
    comp = CompareInventories(inv1, inv2)
    comp.compare()