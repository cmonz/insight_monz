import tempfile
import itertools
import heapq

LINES_HOLD_IN_MEM = 1000000
FILE_OUT = 'tweet_output/ft2.txt'
FILE_IN = 'tweet_input/tweets.txt'
MEDIAN_LIST = []

def get_batch_it(file_name = FILE_IN):
    file_handle = open(file_name, 'rb')
    it = itertools.groupby(file_handle, key=lambda k,
                           line=itertools.count(): next(line) // LINES_HOLD_IN_MEM)
    return it, file_handle

def close_file(file_handle):
    return file_handle.close()

def count_unique(line):
    return len(set(line[:-1].split(' ')))

def write_median(batch_it):
    with open(FILE_OUT, 'w') as fout:
        for k, group in batch_it:
            for line in group:
                MEDIAN_LIST.append(count_unique(line))
                median_len = len(MEDIAN_LIST)
                result = 0
                if median_len % 2 == 0:
                    result = (MEDIAN_LIST[median_len // 2] + MEDIAN_LIST[(median_len // 2) - 1]) / 2.0
                else:
                    result = MEDIAN_LIST[median_len // 2]
                fout.write(str(result) + '\n')
        
def main():

    batch_it, raw_file_handle = get_batch_it()
    write_median(batch_it)
    close_file(raw_file_handle)

    print 'Median calculated in batches and output to ', FILE_OUT



if __name__ == '__main__':
    main()



