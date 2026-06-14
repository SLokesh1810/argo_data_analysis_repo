import chromadb
import pandas as pd
from sentence_transformers import SentenceTransformer

def store_vector_db(updated_df: pd.DataFrame, updated_floats: set):
    model = SentenceTransformer('all-MiniLM-L6-v2')
    client = chromadb.PersistentClient(path="./chroma_db")
    
    collection = client.get_or_create_collection("argo_profiles")
    
    if updated_df.empty:
        print("No updated floats found.")
        return

    documents = updated_df["summary"].tolist()
    ids = updated_df["float_id"].astype(str).tolist()

    embeddings = model.encode(
        documents,
        show_progress_bar=True
    )

    metadata = []
    for _, row in updated_df.iterrows():
        metadata.append({
            "float_id": str(row["float_id"]),
            "num_profiles": int(row["num_profiles"]),
            "num_rows": int(row["num_rows"]),
            "lat_min": float(row["latmin"]),
            "lat_max": float(row["latmax"]),
            "lon_min": float(row["lonmin"]),
            "lon_max": float(row["lonmax"])
        })

    # Upsert replaces existing vectors
    collection.upsert(
        ids=ids,
        documents=documents,
        embeddings=embeddings.tolist(),
        metadatas=metadata
    )