# centralized ADC study list

cache_file = 'cache_lists/adc_cache_list.txt'
exclude_file = 'cache_lists/adc_cache_exclude.txt'

def read_list_from_file():
    with open(cache_file, 'r') as file:
        cache_list = [line.strip() for line in file]

    with open(exclude_file, 'r') as file:
        exclude_list = [line.strip() for line in file]

    remove_set = set(exclude_list)
    # Keep items only if they are NOT in remove_set
    result = [item for item in cache_list if item not in remove_set]

    return result

if __name__ == "__main__":
    cache_list = read_list_from_file()
    print(' '.join(cache_list))
