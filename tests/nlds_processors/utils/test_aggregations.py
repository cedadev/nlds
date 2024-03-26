import pytest

from nlds_processors.utils.aggregations import aggregate_files

class MockFile():
    name: str
    size: int

    def __init__(self, name, size):
        self.name = name
        self.size = size

    def __repr__(self):
        return str(self.size)

def compare_total_length(original, result):
    # Check that no files have been missed from original list
    assert len(original) == sum([len(agg) for agg in result])

def test_aggregate_files_empty_list():
    filelist = []
    with pytest.raises(ZeroDivisionError):
        result = aggregate_files(filelist)

def test_aggregate_files_single_file():
    file1 = MockFile("file1", 100)
    filelist = [file1]
    result = aggregate_files(filelist)
    assert len(result) == 1
    assert len(result[0]) == 1


class TestAggregationsSmallFiles:

    @pytest.fixture
    def sample_small_files(self):
        file1 = MockFile("file1", 100)
        file2 = MockFile("file2", 200)
        file3 = MockFile("file3", 300)
        file4 = MockFile("file4", 400)
        return [file1, file2, file3, file4]

    def test_aggregate_files_basic(self, sample_small_files):
        result = aggregate_files(sample_small_files)
        assert len(result) == 1
        assert len(result[0]) == 4
        compare_total_length(sample_small_files, result)

    def test_aggregate_files_custom_target_count(self, sample_small_files):
        result = aggregate_files(sample_small_files, target_agg_count=2)
        assert len(result) == 2
        assert len(result[0]) <= 2
        assert len(result[1]) <= 2
        compare_total_length(sample_small_files, result)

    def test_aggregate_files_small_target_count(self, sample_small_files):
        result = aggregate_files(sample_small_files, target_agg_count=1)
        assert len(result) == 1
        assert len(result[0]) == 4
        compare_total_length(sample_small_files, result)


class TestAggregationsLargeFiles:

    @pytest.fixture
    def sample_large_files(self):
        large_files = []
        for siz in range(1, 15):
            file_size = int(10**(siz))
            large_files.append(MockFile(f"file-{siz}", file_size))
        return large_files
    

    def test_aggregate_files_basic(self, sample_large_files):
        result = aggregate_files(sample_large_files)
        print(result)
        assert len(result) == 5
        assert len(result[0]) == 1
        assert len(result[1]) == 1
        assert len(result[2]) == 1
        assert len(result[3]) == 1
        assert len(result[4]) == 10
        compare_total_length(sample_large_files, result)

    def test_aggregate_files_custom_target_count(self, sample_large_files):
        result = aggregate_files(sample_large_files, target_agg_count=2)
        assert len(result) == 2
        assert len(result[0]) <= 2
        assert len(result[1]) <= 13
        compare_total_length(sample_large_files, result)

    def test_aggregate_files_small_target_count(self, sample_large_files):
        result = aggregate_files(sample_large_files, target_agg_count=1)
        assert len(result) == 1
        assert len(result[0]) == 14
        compare_total_length(sample_large_files, result)

    def test_aggregate_files_large_target_count(self, sample_large_files):
        result = aggregate_files(sample_large_files, target_agg_count=7)
        assert len(result) == 7
        assert len(result[0]) == 1
        assert len(result[1]) == 1
        assert len(result[2]) == 1
        assert len(result[3]) == 1
        assert len(result[4]) == 1
        assert len(result[5]) == 1
        assert len(result[6]) == 8
        compare_total_length(sample_large_files, result)

    def test_aggregate_files_custom_target_size_10MB(self, sample_large_files):
        target_size = 10*(1000**2) # 10MB
        result = aggregate_files(sample_large_files, target_agg_size=target_size)
        assert len(result) == 5
        assert len(result[0]) == 1
        assert len(result[1]) == 1
        assert len(result[2]) == 1
        assert len(result[3]) == 1
        assert len(result[4]) == 10
        compare_total_length(sample_large_files, result)

    def test_aggregate_files_custom_target_size_10GB(self, sample_large_files):
        target_size = 10*(1000**3) # 10GB
        result = aggregate_files(sample_large_files, target_agg_size=target_size)

        # This is unchanged from the above as it's only controlled by the number 
        # of bins with this size set.
        assert len(result) == 5
        assert len(result[0]) == 1
        assert len(result[1]) == 1
        assert len(result[2]) == 1
        assert len(result[3]) == 1
        assert len(result[4]) == 10
        compare_total_length(sample_large_files, result)


# TODO: Make some more of these with different sizes, not just orders of 
#       magnitude