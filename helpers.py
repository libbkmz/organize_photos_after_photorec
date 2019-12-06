from dataclasses import dataclass
import imagehash

@dataclass
class Hashes:
    file_path: str
    avg_hash: imagehash.ImageHash
    avg_hash_long: imagehash.ImageHash
    p_hash: imagehash.ImageHash

    exif_data: dict
