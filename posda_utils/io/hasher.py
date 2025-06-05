import hashlib

def hash_file(filename, buffer_size = 8192):
    """Calculate the md5sum of file, return (size, digest)"""
    file_size = 0
    file_hash = hashlib.md5()
    
    with open(filename, "rb") as f:
        while (chunk := f.read(buffer_size)):
            file_hash.update(chunk)
            file_size += len(chunk)

    return file_size, file_hash.hexdigest()


def hash_data(data):
    """Calculate the md5sum of data, return (size, digest)"""
    if isinstance(data, str):
        # Encode the string to bytes
        data = data.encode()

    data_hash = hashlib.md5(data)
    return len(data), data_hash.hexdigest()


def hash_uid(uid, uid_root = "1.3.6.1.4.1.14519.5.2.1", trunc = 64):
    """Generate a hashed UID using MD5 digest."""
    if uid.startswith(uid_root):
        return uid
    md5 = hashlib.md5(uid.encode())
    return f"{uid_root}.{int(md5.hexdigest(), 16)}"[:trunc]


def hash_uid_list(uid_list, uid_root = "1.3.6.1.4.1.14519.5.2.1", trunc = 64):
    """Hash a list of UIDs using hash_uid()."""
    return [(uid, hash_uid(uid, uid_root, trunc)) for uid in uid_list]