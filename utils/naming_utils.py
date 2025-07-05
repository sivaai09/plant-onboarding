import re

def generate_new_name(old_name: str, source_plant: str, target_plant: str) -> str:
    """Generates a new table or view name for the target plant."""
    return old_name.replace(source_plant, target_plant)
