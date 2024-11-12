import heapq
import os
import random
import math 
'''important to note, we read a number of files N, 
and we assume that each file is filled with integers, each on
a new line.
'''
total_runs = 0

def read_and_sort_chunks(file_name, chunk_size):
    sorted_runs = []
    with open(file_name, 'r') as file:
        while True:
            chunk = []
            # read chunk of data
            for _ in range(chunk_size):
                line = file.readline()
                if not line:
                    break
                chunk.append(int(line.strip()))
            if not chunk:
                break
            sorted_runs.append(sorted([chunk]))
    return sorted_runs


def ideal_distribution(n, total_runs):
    distribution = [1] * (n - 1)  # Initialize with 1s
    current_sum = sum(distribution)  # Initialize current_sum
    should_merge_count = 0  # Counter for how many times we should merge

    #print(f"Initial distribution: {distribution}, current_sum: {current_sum}")

    # Loop until current_sum is greater than total_runs
    while current_sum < total_runs:
        largest = max(distribution)
        largest_index = distribution.index(largest)

        # Increment each index by the largest value except the largest itself
        for i in range(len(distribution)):
            if i != largest_index:
                distribution[i] += largest

        current_sum = sum(distribution)  # Update current_sum after incrementing
        

        # Increment the counter for how many times we should merge
        should_merge_count += current_sum
    #print(f'Should merge {should_merge_count - 1} times')
    #print(f"Final distribution: {distribution}, current_sum: {current_sum}")
    return distribution

def distribute_runs(runs, num_tapes):
    # Get distribution
    distribution = ideal_distribution(num_tapes, len(runs))
    # Ensure the last tape is empty
    distribution.append(0)  # Set the last tape to have 0 runs

    total_runs_needed = sum(distribution)
    dummy_runs_count = total_runs_needed - len(runs)

    dummy_runs = [[10000]] * dummy_runs_count if dummy_runs_count > 0 else []
    #print(dummy_runs)
    # Initialize tapes
    tapes = [[] for _ in range(num_tapes)]

    run_index = 0  # Track current run being distributed

    # Distribute runs across tapes based on the distribution
    for tape_index in range(num_tapes):
        for _ in range(distribution[tape_index]):
            if run_index < len(runs):
                tapes[tape_index].append(runs[run_index][0])
                run_index += 1
            else:
                tapes[tape_index].append(dummy_runs.pop(0) if dummy_runs else [10000])
    return tapes


def merge_sorted_runs(tapes):
    merged_output = []
    min_heap = []
    global total_runs
    # Initialize the heap with the first element of each valid run
    #print(tapes)
    for tape in tapes:
        heapq.heappush(min_heap, (tape[0], tape, 0))
                    
    #print(f"Initial heap: {min_heap}")  # Debug statement

    # Merge runs until the heap is empty
    counter = 0
    while min_heap:
        #print(f"Heap before popping: {min_heap}")  # Debug statement
        value, run, index = heapq.heappop(min_heap)
        merged_output.append(value)
        counter += 1
        # If there's a next element in the current run, push it into the heap
        #print(run)
        #print(len(run))
        if index + 1 < len(run):  # Check if there's a next element
            heapq.heappush(min_heap, (run[index + 1], run, index + 1))
            #print(f"Pushed to heap: {(run[index + 1], run, index + 1)}")  # Debug statement

    #print(f"Merged output: {merged_output}")  # Debug statement
    total_runs += counter
    return [merged_output]  # Return the merged output directly

def recursive_merge(tapes):

    '''
    Input Type: [[Tape1], [Tape2]]

    Output Type: 
    '''


    # Base case: If only one tape has runs, return it
    if sum(len(tape) > 0 for tape in tapes) == 1:
        #print(f'Tapes at the end of merging: {tapes}')
        return tapes[0]
    # Calculate the minimum runs to merge
    min_runs = min(len(tape) for tape in tapes[:-1] if len(tape) > 0)
    #print(f"Min runs to merge: {min_runs}")  # Debugging statement
    # Prepare to merge only the min_runs from the n-1 tapes
    for i in range(min_runs):
        runs_to_merge = [tape[0] for tape in tapes[:-1] if tape]
        #print(f'runs to merge: {runs_to_merge}')
        # Debug output to confirm the structure before merging
        #print("Before merging:", tapes)
        # Call the merge_sorted_runs function with the current tapes
        merged_runs = merge_sorted_runs(runs_to_merge)
        # Append merged runs to the last tape
        if isinstance(merged_runs, list):  # Ensure merged_runs is a list
            tapes[-1].extend(merged_runs)  # Maintain the list structure
        else:
            tapes[-1].extend([merged_runs])  # Wrap in a list if not
    # Append merged runs to the last tape
        #print(f'Merged runs: {merged_runs}')

        #print(f'tape -1: {tapes[-1]}')
    # Remove min_runs from each of the tapes that were merged
        for i in range(len(tapes) - 1):
          # All but the last tape
            tapes[i] = tapes[i][1:]  # Keep only the remaining runs

        

    # Remove empty tapes
    tapes = [tape for tape in tapes if tape]  # Filter out empty tapes
    tapes.append([])  # Append an empty tape for future merges
    #print(f"Tapes after merging: {tapes}")  # Debugging statement
    # Recursive call to merge the remaining runs

    return recursive_merge(tapes)


def polyphase_merge_sort(file_list, chunk_size, num_tapes):

    '''
        Return Type: 
    '''
    all_runs = []
    for file_name in file_list:
        sorted_runs = read_and_sort_chunks(file_name, chunk_size)
        all_runs.extend(sorted_runs)
    #print(f'all runs: {all_runs}')

    # Distribute runs across tapes
    tapes = distribute_runs(all_runs, num_tapes)
    #print(f"Initial Tapes: {tapes}")  # Debugging statement

    total_merges = 0  # To keep track of total merges

    sorted_output = recursive_merge(tapes)
    print(f'Total Runs Moved: {total_runs}')
    print(f'Number Of Runs: {len(all_runs)}')
    reduction_factor = math.exp(len(all_runs) * math.log(len(all_runs)) / total_runs) if total_runs > 0 else 1
    print(f"Reduction factor: {reduction_factor:.2f}")  # Output reduction factor
    #print(f"Total merges of minimum runs: {total_merges}")  # Output the merge count
    #print(tapes)
    return sorted_output

'''
def write_test_files():
    with open('file1.txt', 'w') as f:
        f.write('\n'.join(str(x) for x in [5, 3, 8, 1, 4, 7, 123, 422, 20, 91]))  # Unsorted data
    with open('file2.txt', 'w') as f:
        f.write('\n'.join(str(x) for x in [9, 2, 6, 7, 0, 9, 99, 103, 100, 923, 100]))  # Unsorted data
'''

def write_random_test_file(file_name, num_records, max_value):
    with open(file_name, 'w') as f:
        random_numbers = [random.randint(0, max_value) for _ in range(num_records)]
        f.write('\n'.join(str(num) for num in random_numbers))
    return random_numbers  # Return the list for verification
'''
num_tapes = 5
chunk_size = 1
def test_polyphase_merge_sort(chunk_size, num_tapes):
    write_test_files()  # Create test files
    file_list = ['file1.txt', 'file2.txt']  # List of files to sort
    list_with_blank = polyphase_merge_sort(file_list, chunk_size, num_tapes)
    print(list_with_blank)
    sorted_result = list_with_blank[-1]
    # Expected sorted output
    expected_result = sorted([5, 3, 8, 1, 4, 7, 123, 422, 20, 91, 9, 2, 6, 7, 0, 9, 99, 103, 100, 923, 100])
    sorted_result = [dummy for dummy in sorted_result if dummy != 10000]
    # Assert the result matches the expected output
    assert sorted_result == expected_result, f"Test failed: {sorted_result} != {expected_result}"
    print("Test passed! Sorted Output:", sorted_result)
'''

def test_random_files(chunk_size, num_tapes):
    
    num_records = sum(ideal_distribution(num_tapes, 111111))
    max_value = 100  # Max random value
    file_name = f'random_file.txt'
    original_numbers = write_random_test_file(file_name, num_records, max_value)
    list_with_blanks = polyphase_merge_sort([file_name], chunk_size, num_tapes)
    # Sort the original numbers for expected result
    expected_result = sorted(original_numbers)
    sorted_result = list_with_blanks[-1]
    # Assert the result matches the expected output
    sorted_result = [dummy for dummy in sorted_result if dummy != 10000]
    assert sorted_result == expected_result, f"Random test failed for {file_name}: {sorted_result} != {expected_result}"
    print(f'Num Tapes:{num_tapes}')
    # print(f'Random Test Passed! Unsorted Input:{original_numbers}')
    # Clean up the created test file
    os.remove(file_name)
'''
def write_test_files_for_article():
    # Writing files that will create 16 single record runs
    with open('file1.txt', 'w') as f:
        f.write('\n'.join(str(x) for x in [1, 2, 3, 4]))  # 4 records
    with open('file2.txt', 'w') as f:
        f.write('\n'.join(str(x) for x in [5, 6, 7, 8]))  # 4 records
    with open('file3.txt', 'w') as f:
        f.write('\n'.join(str(x) for x in [9, 10, 11, 12]))  # 4 records
    with open('file4.txt', 'w') as f:
        f.write('\n'.join(str(x) for x in [13, 14, 15, 16]))  # 4 records

def test_polyphase_merge_sort_three_tapes():
    write_test_files_for_article()  # Create test files
    file_list = ['file1.txt', 'file2.txt', 'file3.txt', 'file4.txt']  # List of files to sort
    chunk_size = 1  # Sort one record at a time
    num_tapes = 3 # Using 4 tapes
    
    list_with_blank = polyphase_merge_sort(file_list, chunk_size, num_tapes)
    sorted_result = list_with_blank[-1]  # The last tape should contain the sorted runs
    expected_result = sorted([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])
    #print(sorted_result)
    sorted_result = [dummy for dummy in sorted_result if dummy != 10000]
    #print(sorted_result)
    assert sorted_result == expected_result, f"Test failed: {sorted_result} != {expected_result}"
    #print("Article Example Test passed! Sorted Output:", sorted_result)
'''
if __name__ == "__main__":
    #test_polyphase_merge_sort(chunk_size, num_tapes)
    i=8
    while i > 2:
        test_random_files(1, i)
        total_runs = 0
        i-=1
    #test_polyphase_merge_sort_three_tapes()
    total_runs = 0



'''
[1,0,0,0]
[1,1,1,0]
[0.2.2.1]
[2,4,3,0]
[6,0,7,4]

'''