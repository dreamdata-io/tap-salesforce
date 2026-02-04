"""
Run with: python test_merge.py
"""


# ============================================================
# SETUP
# ============================================================
class Table:
    def __init__(self, primary_key):
        self.primary_key = primary_key


class PrimaryKeyNotMatch(Exception):
    pass


# ============================================================
# OLD LOGIC (broken)
# ============================================================
def merge_records_old(paginators, table):
    for records in zip(*paginators):
        merged = {}
        pk = None
        for record in records:
            if not pk:
                pk = record[table.primary_key]
            if pk != record[table.primary_key]:
                raise PrimaryKeyNotMatch(
                    f"Mismatch: {pk} vs {record[table.primary_key]}"
                )
            merged.update(record)
        yield merged


# ============================================================
# NEW LOGIC (fixed)
# ============================================================
def merge_records_new(paginators, table):
    merged = {}
    order = []
    for i, paginator in enumerate(paginators):
        for record in paginator:
            pk = record[table.primary_key]
            if pk not in merged:
                merged[pk] = {}
                if i == 0:
                    order.append(pk)
            merged[pk].update(record)
    for pk in order:
        yield merged[pk]


# ============================================================
# TEST DATA
# ============================================================
table = Table(primary_key="Id")


# Paginator A: returns AAA, BBB, CCC
def pag_a():
    return iter(
        [
            {"Id": "AAA", "Name": "Acme"},
            {"Id": "BBB", "Name": "Beta"},
            {"Id": "CCC", "Name": "Gamma"},
        ]
    )


# Paginator B: returns BBB, AAA, CCC (DIFFERENT ORDER!)
def pag_b():
    return iter(
        [
            {"Id": "BBB", "Industry": "Finance"},
            {"Id": "AAA", "Industry": "Tech"},
            {"Id": "CCC", "Industry": "Health"},
        ]
    )


# ============================================================
# RUN TESTS
# ============================================================
print("=" * 50)
print("TEST 1: OLD logic with mismatched order")
print("=" * 50)
try:
    result = list(merge_records_old([pag_a(), pag_b()], table))
    print("UNEXPECTED: Old logic passed")
except PrimaryKeyNotMatch as e:
    print(f"EXPECTED FAILURE: {e}")

print()
print("=" * 50)
print("TEST 2: NEW logic with mismatched order")
print("=" * 50)
try:
    result = list(merge_records_new([pag_a(), pag_b()], table))
    print("SUCCESS: New logic handled it correctly!")
    for r in result:
        print(f"  {r}")
except PrimaryKeyNotMatch as e:
    print(f"UNEXPECTED FAILURE: {e}")
