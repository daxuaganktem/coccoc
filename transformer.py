import gzip
import ast
import operator
import matplotlib.pyplot as plt
import numpy as np

# open and import
with gzip.open('hash_catid_count.csv.gz', 'rt') as f:
   columns = list(zip(*(map(str, row.split()) for row in f)))

cat_ids = columns[1]
cat_counts = columns[2]

population_dict = {}
counter_dict = {}

# counting categories
for i in range (len(cat_ids)):
    cat_arr = ast.literal_eval(cat_ids[i])
    cat_count_arr = ast.literal_eval(cat_counts[i])
    for j in range (len(cat_arr)):
        if cat_arr[j] in population_dict:
            population_dict[cat_arr[j]] += 1
            counter_dict[cat_arr[j]] += int(cat_count_arr[j])
        else:
            population_dict[cat_arr[j]] = 1
            counter_dict[cat_arr[j]] = int(cat_count_arr[j])

most_popular_key = max(population_dict.items(), key=operator.itemgetter(1))[0]
most_count_key = max(counter_dict.items(), key=operator.itemgetter(1))[0]

# most popular category
print(most_popular_key, population_dict[most_popular_key])

# most appeared category
print(most_count_key, counter_dict[most_count_key])

#frequency
frequency_dict = {k: v / total for total in (sum(population_dict.values()),) for k, v in population_dict.items()}


#plotting the count - population
plt.figure('By Count - Population figure')
plt.plot(np.array(list(population_dict.values())),np.array(list(counter_dict.values())))
plt.draw()


#plotting the count
plt.figure('By Count figure')
plt.plot(np.array(list(counter_dict.values())))
plt.draw()


# plotting the frequency
plt.figure('By frequency figure')
plt.plot(np.array(list(frequency_dict.values())))

plt.show()

