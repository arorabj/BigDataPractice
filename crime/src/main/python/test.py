# Python3 program to find a triplet using Hashing 
# returns true if there is triplet with sum equal 
# to 'sum' present in A[]. Also, prints the triplet 
def find3Numbers(A, arr_size, sum):
    for i in range(0, arr_size - 1):
        # Find pair in subarray A[i + 1..n-1]  
        # with sum equal to sum - A[i] 
        s = set()
        print s,i
        balance = sum - A[i]
        for j in range(i + 1, arr_size):
            if (balance - A[j]) in s:
                print("Triplet is", A[i],
                      ", ", A[j], ", ", balance - A[j])
                return True
            s.add(A[j])

    return False


# Driver program to test above function
A = [1, 4, 45, 6, 10, 8,2,11,12,9]
sum = 22
arr_size = len(A)
find3Numbers(A, arr_size, sum)
# This is contributed by Yatin gupta 