# Part 1

# Define 5 variables with different values
a = 10
b = 20
c = 15
d = 30
e = 25

# Perform comparisons and print results
print(f"Is a equal to b? {a == b}")
print(f"Is a less than b? {a < b}")
print(f"Is c greater than d? {c > d}")
print(f"Is d equal to a? {d == a}")
print(f"Is e not equal to c? {e != c}")
print(f"Is b greater than or equal to e? {b >= e}")
print(f"Is a less than or equal to d? {a <= d}")
print(f"Is e greater than a? {e > a}")
print(f"Is c less than b? {c < b}")
print(f"Is d greater than or equal to a? {d >= a}")

# Part 2

# Define the variable
status = "Lost in the Void"

# Perform the checks
if status == "pending":
    print("The status is pending.")
elif status == "approved":
    print("The status is approved.")
elif status == "rejected":
    print("The status is rejected.")
elif status == "in_progress":
    print("The status is in progress.")
elif status == "completed":
    print("The status is completed.")
else:
    print("Unknown status.")