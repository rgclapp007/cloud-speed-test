def filter_machine_types(df, min_cpus=None, max_cpus=None, min_memory=None, max_memory=None, exclude_types=None, exclude_patterns=None, min_gpus=None, max_gpus=None):
    """
    Filters the DataFrame based on specified criteria, including the ability to exclude machine types based on patterns.

    Args:
    df (DataFrame): The DataFrame to filter.
    min_cpus (int), max_cpus (int): Minimum and maximum number of CPUs.
    min_memory (float), max_memory (float): Minimum and maximum amount of memory in GiB.
    exclude_types (list): List of exact machine types to exclude.
    exclude_patterns (list): List of patterns to exclude (e.g., all types containing 'micro').
    min_gpus (int), max_gpus (int): Minimum and maximum number of GPUs.

    Returns:
    DataFrame: The filtered DataFrame.
    """
    if min_cpus is not None:
        df = df[df['CPUs'] >= min_cpus]
    if max_cpus is not None:
        df = df[df['CPUs'] <= max_cpus]
    if min_memory is not None:
        df = df[df['Memory (GiB)'] >= min_memory]
    if max_memory is not None:
        df = df[df['Memory (GiB)'] <= max_memory]
    if exclude_types is not None:
        df = df[~df['Machine Type'].isin(exclude_types)]
    if exclude_patterns is not None:
        if isinstance(exclude_patterns,list):
            pattern_regex = "|".join(exclude_patterns)  # Combine all patterns into a single regex
        else:
            pattern_regex=exclude_patterns
        df = df[~df['Machine Type'].str.contains(pattern_regex, case=False, regex=True)]

    if min_gpus is not None:
        df = df[df['GPUs'] >= min_gpus]
    if max_gpus is not None:
        df = df[df['GPUs'] <= max_gpus]

    return df

