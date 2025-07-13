import re

def generate_new_name(old_name: str, source_plant: str, target_plant: str) -> str:
    """Generates a new table or view name for the target plant."""
    # Use a regular expression to replace the source plant name when it's followed by an underscore
    # or is at the end of the string. This handles cases like 'plant1_orders' or 'plant1'.
    pattern = re.escape(source_plant) + r'(_|$)'
    return re.sub(pattern, target_plant + r'\1', old_name)