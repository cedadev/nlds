from test_aggregations import MockFile
from random import uniform
import os.path
from uuid import uuid4
from nlds_processors.utils.aggregations import bin_files_1, bin_files_2, bin_files
from time import perf_counter


def generate_random_files(number, size_min, size_max):
    """Generate a number of MockFiles"""
    name_stub = "/gws/nopw/j04/cedaproc"
    filelist = []
    for n in range(0, number):
        size = int(uniform(size_min, size_max))
        rando_name = os.path.join(name_stub, str(uuid4()))
        file = MockFile(name=rando_name, size=size)
        filelist.append(file)

    return filelist


def bin_size(bin):
    s = 0
    for f in bin:
        s += f.size
    return s


def bin_min(bin):
    m = 2e20
    for f in bin:
        if f.size < m:
            m = f.size
    return m


def bin_max(bin):
    m = 2e-20
    for f in bin:
        if f.size > m:
            m = f.size
    return m


def run_test(number, size_min, size_max, target_bin_count, target_bin_size):
    flist = generate_random_files(number, size_min=size_min, size_max=size_max)
    st = perf_counter()
    bins = bin_files(
        flist, target_bin_size=target_bin_size, target_bin_count=target_bin_count
    )
    et = perf_counter()
    for c, b in enumerate(bins):
        print(c, ":", len(b), bin_size(b), bin_min(b), bin_max(b))
    print("Time elapsed", et - st)


def run_test_small_files():
    number = 32000
    size_min = 100 * 1024  # kilobytes
    size_max = 200 * 1024
    target_bin_size = 5000000000  # (5 GB) - this is from the NLDS config settings
    target_bin_count = 1000
    print("run_test_small_files")
    run_test(number, size_min, size_max, target_bin_count, target_bin_size)


def run_test_large_files():
    number = 100
    size_min = 4 * 1024 * 1024 * 1024
    size_max = 100 * 1024 * 1024 * 1024
    target_bin_size = 5000000000  # (5 GB) - this is from the NLDS config settings
    target_bin_count = 1000
    print("run_test_large_files")
    run_test(number, size_min, size_max, target_bin_count, target_bin_size)


def run_test_mixed_files():
    number = 100
    size_min = 100 * 1024  # kilobytes
    size_max = 100 * 1024 * 1024 * 1024
    target_bin_size = 5000000000  # (5 GB) - this is from the NLDS config settings
    target_bin_count = 1000
    print("run_test_mixed_files")
    run_test(number, size_min, size_max, target_bin_count, target_bin_size)


def run_test_small_and_large_files():
    # model a potential real world scenario:
    # a mix of small files, like manifests and metadata plus
    # a mix of large files, like actual data
    small_files = generate_random_files(1000, size_min=1 * 1024, size_max=1000 * 1024)
    big_files = generate_random_files(
        1000, size_min=10 * 1024 * 1024 * 1024, size_max=100 * 1024 * 1024 * 1024
    )
    flist = small_files + big_files

    target_bin_size = 5000000000  # (5 GB) - this is from the NLDS config settings
    target_bin_count = 1000
    st = perf_counter()
    bins = bin_files_2(
        flist, target_bin_size=target_bin_size, target_bin_count=target_bin_count
    )
    print("run_test_small_and_large_files")
    et = perf_counter()
    for c, b in enumerate(bins):
        print(c, ":", len(b), bin_size(b), bin_min(b), bin_max(b))
    print("Time elapsed", et - st)

if __name__ == "__main__":
    run_test_small_files()
    run_test_large_files()
    run_test_mixed_files()
    run_test_small_and_large_files()