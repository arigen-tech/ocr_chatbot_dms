from fastapi import FastAPI, Query, HTTPException, File, UploadFile
from pydantic import BaseModel
from typing import List, Optional
import uvicorn
from Models.ocr_search_model import DocumentProcessor
from Models.ai_chatbot_model import ChatbotProcessor
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager


class SearchRequest(BaseModel):
    query: str
    selected_files: Optional[List[str]] = None

class ChatRequest(BaseModel):
    message: str

app = FastAPI(
    lifespan=lambda app: lifespan(app)
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000","https://arigen-tech.github.io/dms-ui", "http://52.66.126.151"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_processor():
    """Get the DocumentProcessor instance."""
    processor = DocumentProcessor.get_instance()
    if not processor:
        raise HTTPException(status_code=500, detail="Document Processor not initialized")
    return processor

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start the model processing in the background."""
    doc_processor = get_processor()
    doc_processor.start_processing()

    chatbot_processor = ChatbotProcessor.get_instance()  # Initialize chatbot processor
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

@app.post("/search/selected")
async def search_in_selected_files(request: SearchRequest):
    """API to search in selected PDF files."""
    try:
        processor = get_processor()
        results = processor.search_database(request.query, request.selected_files)
        return {"query": request.query, "matching_files": results}
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
    uvicorn.run(app, host="0.0.0.0", port=8000)

if __name__ == "__main__":
    start_api_server()