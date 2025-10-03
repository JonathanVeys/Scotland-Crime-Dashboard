import numpy as np


def find_left(arr, low, high, target):
    while low <= high:
        mid = (low + high) // 2
        if arr[mid] == target:
            if mid == 0 or arr[mid-1] < target:
                return mid
            else:
                high = mid - 1
        elif arr[mid] < target:
            low = mid + 1
        else:
            high = mid - 1
    return -1   # not found
    
def find_left_2(arr, low, high, target):
    if low>high:
        return low
    mid = (low+high)//2
    
    if arr[mid] == target:
        return find_left_2(arr, low, mid, target)
    if arr[mid] < target:
        return find_left(arr, mid, high, target)
            
    

def find_right(arr, low, high, target):
    if low>=high:
        return high
    mid = (low+high)//2

    if arr[mid] == target:
        return find_right(arr, mid+1, high, target)
    elif arr[mid] > target:
        return find_right(arr, low+1, mid, target)
    


arr = [1,2,3,3,4,4,4,4,4,8]

print(find_left_2(arr, low=0, high=(len(arr)//2), target=4))
print(find_right(arr, low=(len(arr)//2), high=len(arr)-1, target=4))


