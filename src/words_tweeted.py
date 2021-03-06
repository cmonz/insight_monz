import tempfile
import itertools
import heapq
import sys

LINES_HOLD_IN_MEM = 1000000

def get_batch_it(file_name):
    file_handle = open(file_name, 'rb')
    it = itertools.groupby(file_handle, key=lambda k,
                           line=itertools.count(): next(line) // LINES_HOLD_IN_MEM)
    return it, file_handle

def close_file(file_handle):
    return file_handle.close()


def merge_write(file_name, tempFiles):
    with open(file_name, 'w') as dest:
    
        merged = heapq.merge(*tempFiles)
        past_item = merged.next()
        counter = 1
        
        for item in merged:
            if item != past_item:
                dest.write(past_item[:-1] + '\t' + str(counter) + '\n')
                past_item = item
                counter = 1
            else:
                counter += 1 
                
        dest.write(past_item[:-1] + '\t' + str(counter) + '\n')
        

def close_files(file_list):
    for each in file_list:
        each.close()
    
def write_sorted_batches(batch_it, temp_list):
    for k, group in batch_it:
        sorted_chunk = sorted((''.join(group)).replace('\r\n', ' ').split(' '))[1:]
        temp = tempfile.TemporaryFile()
        for item in sorted_chunk:
            temp.write(item + '\n')
        temp_list.append(temp)
        del sorted_chunk
        temp.seek(0)

    return temp_list
        

def main():

    file_out = 'tweet_output/ft1.txt'
    file_in = 'tweet_input/tweets.txt'
    if len(sys.argv) == 3:
        file_in = sys.argv[1][2:]
        file_out = sys.argv[2][2:]
    
    batch_it, raw_file_handle = get_batch_it(file_in)
    temp_files = []
    temp_files = write_sorted_batches(batch_it, temp_files)
    merge_write(file_out, temp_files)
    close_files(temp_files)
    close_file(raw_file_handle)

    print 'Tweets sorted and output to ', file_out



if __name__ == '__main__':
    main()



