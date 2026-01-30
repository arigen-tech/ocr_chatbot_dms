from fastapi import FastAPI, Query, HTTPException, File, UploadFile
from pydantic import BaseModel
from typing import List, Optional
import uvicorn
import os
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import json
import base64
from fastapi.responses import FileResponse

from Models.ocr_search_model_1 import DocumentProcessor # when you want to use the original ocr model (Preffered)
# from Models.ocr_search_model import DocumentProcessor # when you want to use the FTP version of the ocr model

from Models.ai_chatbot_model import ChatbotProcessor #use when you want to use SBERT LLM
# from Models.ai_model import ChatbotProcessor #use when you want to use Ollama LLM(more smater then BERT)
# from Models.ocr_search_model_1 import RemoteFileManager

from Models.document_scaler import DocumentScaler

"""
Commands to run when applying Ollama 
1. ollama serve
2. ollama run gemma3:1b
"""

class SearchSelected(BaseModel):
    query: str
    mysql_original_id: List[int]

class ChatRequest(BaseModel):
    message: str

class ScaleDocumentRequest(BaseModel):
    file_name: str
    scale_type: int  # 1 for upscale, 0 for downscale

app = FastAPI(
    lifespan=lambda app: lifespan(app)
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000","https://arigen-tech.github.io/dms-ui", "http://52.66.126.151", "https://103.133.215.182"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# remote_manager = RemoteFileManager()
# remote_manager=remote_manager

DB_URL = 'mysql+pymysql://root:ishan2911@localhost:3306/dms'
# DB_URL = 'mysql+pymysql://dms:DMS123@103.133.215.182:3306/dms'

# Encryption key for decrypting files (16 bytes for AES-128)
def get_encryption_key():
    """
    Get encryption key from environment variable or use default.
    Handles both Base64-encoded and raw byte formats.
    """
    key_env = os.getenv("ENCRYPTION_KEY", "uG1w6s0ZzXgE6Zx4xq9D0w==")
    
    try:
        # Try to decode as Base64 first (standard format from Java programs)
        key_bytes = base64.b64decode(key_env)
        
        # Verify it's 16 bytes (AES-128)
        if len(key_bytes) == 16:
            return key_bytes
        else:
            print(f"Warning: Base64-decoded key is {len(key_bytes)} bytes, expected 16. Truncating/padding...")
            return key_bytes[:16].ljust(16, b'\0')
    except Exception:
        # If Base64 decode fails, treat as raw UTF-8 text
        key_bytes = key_env.encode('utf-8')[:16].ljust(16, b'\0')
        print(f"Using UTF-8 encoded key (not Base64 decoded)")
        return key_bytes

ENCRYPTION_KEY = get_encryption_key()

def get_processor():
    """Get the DocumentProcessor instance."""
    processor = DocumentProcessor.get_instance(
        db_url=DB_URL,
        encryption_key=ENCRYPTION_KEY  # ← Encryption key passed here
    )
    if not processor:
        raise HTTPException(status_code=500, detail="Document Processor not initialized")
    return processor

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start the model processing in the background."""
    doc_processor = get_processor()
    doc_processor.start_processing()

    # chatbot_processor = ChatbotProcessor.get_instance()  # Initialize chatbot processor
    yield

@app.get("/")
async def root():
    """Simple root endpoint to verify if the API is running."""
    return {"message": "OCR-Search Engine is running..."}

@app.get("/files")
async def get_files():
    """API to get all available Documents."""
    try:
        processor = get_processor()
        files = processor.get_all_documents()
        return {"files": files}
    except Exception as e:
        return {"error": str(e)}

@app.get("/failed-files")
async def get_failed_files():
    """API to get the list of failed/corrupted files from failed_files.json."""
    processor = DocumentProcessor.get_instance()
    failed_log_path = processor.db_path.parent / "failed_files.json"
    try:
        if not failed_log_path.exists():
            return {"failed_files": []}
        with open(failed_log_path, "r", encoding="utf-8") as f:
            failed_files = json.load(f)
        return {"failed_files": failed_files}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/search/selected")
async def search_in_selected_files(request: SearchSelected):
    """API to search in selected PDF files."""
    try:
        processor = get_processor()

        results = processor.search_database(
            query = request.query, 
            selected_files= request.mysql_original_id
        )

        return {
            "query": request.query,
            "id" : request.mysql_original_id,
            "matching_files": results
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/search/all")
async def search_database(query: str = Query(..., description="Search query")):
    """API to search in all Documents."""
    try:
        processor = get_processor()
        results = processor.search_database(query)
        return {"query": query, "matching_files": results}
    except Exception as e:
        return {"error": str(e)}

@app.post("/clean")
async def clean_database():
    """API to clean the database."""
    try:
        processor = get_processor()
        processor.clean_database()
        return {"message": "Database cleaned successfully."}
    except Exception as e:
        return {"error": str(e)}

# ==================================================Document Scaling endpoints=====================================================

@app.post("/scale/document")
async def scale_document(request: ScaleDocumentRequest):
    """
    Scale a document up or down.
    
    Parameters:
    - file_name: Name of the document to scale (or full path)
    - scale_type: 1 for upscale (150%), 0 for downscale (66%)
    """
    try:
        if request.scale_type not in [0, 1]:
            raise HTTPException(status_code=400, detail="scale_type must be 0 (downscale) or 1 (upscale)")
        
        # Initialize document scaler (base_path set to current working directory)
        scaler = DocumentScaler()
        
        # Scale the document (will search in base_path if needed)
        scaled_path = scaler.scale_file(request.file_name, request.scale_type)
        
        # Get file size
        file_size = os.path.getsize(scaled_path)
        
        return {
            "message": "Document scaled successfully",
            "file_path": scaled_path,
            "file_name": os.path.basename(scaled_path),
            "original_name": request.file_name,
            "scale_type": "upscaled (150%)" if request.scale_type == 1 else "downscaled (66%)",
            "file_size_bytes": file_size,
            "file_size_kb": round(file_size / 1024, 2)
        }
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/scale/download")
async def download_scaled_document(file_path: str = Query(..., description="Path to the scaled document")):
    """
    Download the scaled document.
    
    Parameters:
    - file_path: Full path to the scaled document
    """
    try:
        # Security check: ensure file exists and is within allowed directories
        file_path_obj = os.path.abspath(file_path)
        
        if not os.path.exists(file_path_obj):
            raise HTTPException(status_code=404, detail="Scaled document not found")
        
        file_name = os.path.basename(file_path_obj)
        
        return FileResponse(
            path=file_path_obj,
            filename=file_name,
            media_type='application/octet-stream'
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ==================================================Chat-related endpoints=====================================================
@app.post("/chat/upload")
async def upload_document(file: UploadFile = File(...)):
    """Upload and append a documents for the chatbot."""
    try:
        file_path = f"Documentation/{file.filename}"
        with open(file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)

        # Append the new PDF to the existing chatbot database
        processor = ChatbotProcessor.get_instance()
        success = processor.process_pdf(file_path)

        if success:
            return {"message": "Document added successfully and integrated into chatbot."}
        else:
            raise HTTPException(status_code=400, detail="Failed to process document")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/chat/message")
async def chat_message(request: ChatRequest):
    """Send a message to the chatbot."""
    try:
        processor = ChatbotProcessor.get_instance()
        response = processor.chat(request.message)  # Now supports case-insensitive lookup
        
        if response["response"] == "Please ask questions related to Application.":
            raise HTTPException(status_code=400, detail=response["response"])

        return {"response": response["response"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/chat/clear")
async def clear_chat():
    """Clear the chat history."""
    try:
        processor = ChatbotProcessor.get_instance()
        processor.clear_history()
        return {"message": "Chat history cleared"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/chat/history")
async def get_chat_history():
    """Get the chat history."""
    try:
        processor = ChatbotProcessor.get_instance()
        history = processor.get_chat_history()
        return {"history": history}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def start_api_server():
    """Start the FastAPI server."""
    uvicorn.run(app, host="0.0.0.0", port=8950)

if __name__ == "__main__":
    start_api_server()