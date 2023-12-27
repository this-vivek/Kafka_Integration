# table1
# name,subject1,subject2,subject3

# select name from (select name,dense_rank() over (order by total desc) as  rank from ( select name,(subject1+subject2+subject3) as total from table1)) 
# where rank =5


# table1
# id name
# 1  vivek
# 2 rahul

# table2
# id Name
# 1 vivek
# 3 akash

# 2 rahul
# 3 akash


# select * from table1 outer join table2 on table1.id <> table2.id


# Given an array of integers and a pair sum value,
# return indices of the two numbers so that they sum to the given target pair sum 
# if no such pair exists then return null.

arr = [0,-1,2,-3,1]
sum = -2
d = {}
element1  = arr[0]
# for i in range(0,len(arr)): # 0
#     for j in range(i+1,len(arr)):
#         if i != j and (arr[i] + arr[j] == sum): 
#             print(i,j)

# for i in range(0,len(arr)):
#     d[arr[i]] = i

for i in range(0,len(arr)):
    sub = sum - arr[i]
    if sub in d:
        print(i,d[sub])
    else:
        d[arr[i]] = i



