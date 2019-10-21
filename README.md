# External merge sort

### Problem

Given a large file which can be fully loaded into RAM. The file contains strings separated with some delimiter.
The task is to sort the file. Large file generator should also be implemented. Generator parameters are lines number and
line length.

### Implementation details

The implementation is based on the [external sort algorithm](https://en.wikipedia.org/wiki/External_sorting).
It consists of two steps: split and chunk. 

**Split step**
1. Split the large file into N files each K-bytes size (depends on memory limit).
2. Sort the K-bytes files using some sorting algorithm. For example, merge sort, quick sort, etc in O(n*logn).
3. Store each of the smaller files to external memory (disk).

**Merge step**
1. Do a [K-way merge](https://en.wikipedia.org/wiki/K-way_merge_algorithm)  with each smaller files one by one.
2. After the split step, a list of file handler of all the splitted files will be stored in the map.
3. Сreate a list of heap entries. Each entry will stores the actual data read from the file 
and also the file index which owns it. 
The heap entries is heapified (min heap).
4. Loop while heap is not empty

     4.1. Pick the node with least element from heap (top).
     
     4.2. Write the element to output.
     
     4.3. Find the file handler of the corresponding element by its id by looking at the map of file handlers.
     
     4.4. Read the next part of data from the file . If it is EOF, break.  
     
     4.5. Push the item to heap top. Heapify to persist min heap property.
      
At the end of the merge step the output file will have all the elements in sorted order.

### Example

Given the large file with the following content:
```
4
8
6
1
9
12
```

Split step can result in:
```
tmp_file1:
4
8
temp_file2:
1
6
temp_file3:
9
12
```
We construct the initial heap based on the min element in each file.
The init min heap will look like:
```
                             1                       
                           /  \
                          4    9     
```
Now we peak the least element from the heap and write it to the output.
Then we find the next element in the file which owns the current min element.
In our case it is temp_file2 and the element is 6. We push the element on the heap.
```
          6                                    4
        /  \                                 /  \
       4    9           Heapify -->         6    9	
```
We should continue the above sequence of actions till the heap is not empty.
