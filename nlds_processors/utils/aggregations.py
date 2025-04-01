# encoding: utf-8
"""
aggregations.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "19 Jun 2024"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from typing import List, Union
from ..catalog.catalog_models import File
from nlds.details import PathDetails

DEFAULT_BIN_SIZE = 5 * (1024**3)  # 5 GBs


def bin_files(
    filelist: List[Union[File, PathDetails]],
    target_bin_count: int = None,
    target_bin_size: float = DEFAULT_BIN_SIZE,
) -> List[List[Union[File, PathDetails]]]:
    """Creates a list of groups of files (a bin) from a given list of files. A
    few algorithms for this were explored, with some suiting different
    distributions of file sizes better, so the most generic solution is
    provided. This implementation uses a smallest-first approach, i.e. it
    calculates a target value for bin size and then iterates through the
    Files and sorts each into the smallest bin available at that
    iteration.
    This function is used for producing sets of file lists for:
        1.  Aggregations when creating the aggregations on tape
        2.  Getting files to disk from object storage, to allow for parallel transfers
    """
    if not target_bin_size:
        raise ValueError("target_bin_size must have some value, the default is 5GB")

    # Calculate a target bin count if one is not given
    if target_bin_count is None:
        filesizes = [f.size for f in filelist]
        total_size = sum(filesizes)
        count = len(filesizes)
        mean_size = total_size / count
        if total_size < target_bin_size:
            # If it's less that a single target bin size then just do a single bin
            return [
                filelist,
            ]
        # TODO: Need to think this conditional through a bit more. This is
        # the condition for if all the files are about the size of the
        # target_bin_size, in which case we could end up with
        # len(filelist) bins each with one file in them, which is
        # maximally inefficient for the tape?
        elif mean_size > target_bin_size:
            # For now we'll just set it to 5 in this particular case.
            target_bin_count = 5
        else:
            # Otherwise we're going for the number of target sizes that fit
            # into our total size.
            target_bin_count = int(total_size / target_bin_size)

    if target_bin_count is None:
        raise ValueError("target_bin_count is None")

    # Make 2 lists, one being a list of lists dictating the bins, the
    # other being their sizes, so we're not continually recalculating it
    bins = [[] for _ in range(target_bin_count)]
    sizes = [0 for _ in range(target_bin_count)]
    filelist_sorted = sorted(filelist, reverse=True, key=lambda f: f.size)
    for fs in filelist_sorted:
        # Get the index of the smallest bin
        bin_index = min(range(len(bins)), key=lambda i: sizes[i])
        bins[bin_index].append(fs)
        sizes[bin_index] += fs.size

    return bins
