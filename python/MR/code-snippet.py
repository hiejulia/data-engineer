#step 1
mapped = map(mapper, list_of_strings)
mapped = zip(list_of_strings, mapped)
#step 2:
reduced = reduce(reducer, mapped)
print(reduced)