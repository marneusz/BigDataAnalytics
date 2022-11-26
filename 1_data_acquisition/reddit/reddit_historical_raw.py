

import numpy as np
import os

def main():
    download_links = get_submission_links(from_year=2006, to_year=2006, from_month=3, to_month=4)

    for link in download_links:

        cmd = "curl {0}".format(link)

        os.system(cmd)

        file_name = link.split("/")[-1]

        cmd = "zstd -d --long=31 {0}".format(file_name)
        os.system(cmd)

        cmd = "rm {0}".format(link)
        os.system(cmd)



# from/to_year , from/to_month - integers
def get_submission_links(from_year, to_year, from_month, to_month):
    if from_year <= 2000 or to_year <= 2000 or from_month <= 0 or to_month <= 0 or from_month >= 13 or to_month >= 13:
        raise Exception("invalid year or month value")

    if to_year < from_year:
        raise Exception("invalid years interval: to_year < from_year")
    if to_year == from_year and to_month < from_month:
        raise Exception("invalid months interval: to_month < from_month for the same year")

    submission_links = []

    actual_month = from_month

    for year in range(from_year, to_year + 1):

        # same year
        if year == to_year:

            for month in range(actual_month, to_month + 1):
                #             print(str(year) + '  ' + str(month)  )
                submission_links.append('https://files.pushshift.io/reddit/submissions/RS_' + str(year) + '-' + str(
                    np.where(month < 10, '0' + str(month), str(month))) + '.zst')

            continue

        if year < to_year:

            for month in range(actual_month, 12 + 1):
                #             print(str(year) + '  ' + str(month)  )
                submission_links.append('https://files.pushshift.io/reddit/submissions/RS_' + str(year) + '-' + str(
                    np.where(month < 10, '0' + str(month), str(month))) + '.zst')

            actual_month = 1

    return submission_links


if __name__ == "__main__":
    main()
