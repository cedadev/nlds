from typing import List, Union
from ..catalog.catalog_models import File
from nlds.details import PathDetails

DEFAULT_AGGREGATION_SIZE = 5 * (1024**3)


def aggregate_files(
    filelist: List[Union[File, PathDetails]],
    target_agg_count: int = None,
    target_agg_size: float = DEFAULT_AGGREGATION_SIZE,
) -> List[List[Union[File, PathDetails]]]:
    """Creates a list of suitable aggregations from a given list of files. A
    few algorithms for this were explored, with some suiting different
    distributions of file sizes better, so the most generic solution is
    provided. This implementation uses a smallest-first approach, i.e. it
    calculates a target value for aggregation size and then iterates through the
    Files and sorts each into the smallest aggregation available at that
    iteration.
    """
    if not target_agg_size:
        raise ValueError("target_agg_size must have some value, the default is 5GB")

    # Calculate a target aggregation count if one is not given
    if target_agg_count is None:
        filesizes = [f.size for f in filelist]
        total_size = sum(filesizes)
        count = len(filesizes)
        mean_size = total_size / count
        if total_size < target_agg_size:
            # If it's less that a single target agg size then just do a single
            # aggregation
            return [
                filelist,
            ]
        # TODO: Need to think this conditional through a bit more. This is
        # the condition for if all the files are about the size of the
        # target_aggregation_size, in which case we could end up with
        # len(filelist) aggregations each with one file in them, which is
        # maximally inefficient for the tape?
        elif mean_size > target_agg_size:
            # For now we'll just set it to 5 in this particular case.
            target_agg_count = 5
        else:
            # Otherwise we're going for the number of target sizes that fit
            # into our total size.
            target_agg_count = int(total_size / target_agg_size)

    assert target_agg_count is not None

    # Make 2 lists, one being a list of lists dictating the aggregates, the
    # other being their sizes, so we're not continually recalculating it
    aggregates = [[] for _ in range(target_agg_count)]
    sizes = [0 for _ in range(target_agg_count)]
    filelist_sorted = sorted(filelist, reverse=True, key=lambda f: f.size)
    for fs in filelist_sorted:
        # Get the index of the smallest aggregate
        agg_index = min(range(len(aggregates)), key=lambda i: sizes[i])
        aggregates[agg_index].append(fs)
        sizes[agg_index] += fs.size

    return aggregates
