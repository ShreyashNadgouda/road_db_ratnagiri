from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from pydantic import BaseModel
import geopandas as gpd
import os

# Database configuration
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql+psycopg2://shreeshnadgouda:yash1234@127.0.0.1:5432/testing1')

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

app = FastAPI()

class Query(BaseModel):
    query: str

@app.post("/query")
def execute_query(query: Query):
    try:
        with engine.connect() as connection:
            gdf = gpd.read_postgis(text(query.query), con=connection, geom_col='geom')
        if gdf.crs is None:
            gdf.set_crs(epsg=4326, inplace=True)  # Assuming the fetched data uses WGS84 CRS
        return gdf.to_json()
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/unique-statuses")
def get_unique_statuses():
    try:
        with engine.connect() as connection:
            query = text('SELECT DISTINCT "ratnagiri_final_current_status" FROM "RN_DIV"')
            result = connection.execute(query)
            statuses = [row[0] for row in result]
        return statuses
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
