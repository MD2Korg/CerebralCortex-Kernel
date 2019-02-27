import re
def rename_column_name(column_name):
    return re.sub('[^a-zA-Z0-9]+', '_', column_name).strip("_")