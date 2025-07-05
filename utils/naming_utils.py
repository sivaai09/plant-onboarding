import re

def generate_new_name(old_name: str, source_plant: str, target_plant: str) -> str:
    """Generates a new table or view name for the target plant."""
    # Use a regular expression to ensure we're replacing the plant name as a whole word
    # This avoids accidentally replacing parts of other words
    return re.sub(r'\b' + re.escape(source_plant) + r'\b', target_plant, old_name)