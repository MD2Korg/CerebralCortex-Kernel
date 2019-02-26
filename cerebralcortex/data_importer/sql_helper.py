import json
import mysql
def save_stream_metadata(self, metadata_obj)->dict:
    """
    Update a record if stream already exists or insert a new record otherwise.

    Args:
        metadata_obj (Metadata): stream metadata
    Returns:
        dict: {"status": True/False,"verion":version}
    Raises:
         Exception: if fail to insert/update record in MySQL. Exceptions are logged in a log file
    """
    isQueryReady = 0

    metadata_hash = metadata_obj.get_hash()
    stream_name = metadata_obj.name

    is_metadata_changed = self._is_metadata_changed(stream_name, metadata_hash)
    status = is_metadata_changed.get("status")
    version = is_metadata_changed.get("version")

    metadata_obj.set_version(version)

    metadata_str = metadata_obj.to_json()
    if (status=="exist"):
        return {"status": True,"version":version, "record_type":"exist"}

    if (status == "new"):
        qry = "INSERT INTO " + self.datastreamTable + " (name, version, metadata_hash, metadata) VALUES(%s, %s, %s, %s)"
        vals = str(stream_name), str(version), str(metadata_hash), json.dumps(metadata_str)
        isQueryReady = 1

        # if nothing is changed then isQueryReady would be 0 and no database transaction would be performed
    if isQueryReady == 1:
        try:
            self.execute(qry, vals, commit=True)
            return {"status": True,"version":version, "record_type":"new"}
        except Exception as e:
            raise Exception(e)