import os
import pdfplumber
import time
import threading
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from sentence_transformers import SentenceTransformer, util
import torch

class PDFHandler(FileSystemEventHandler):
    """Handles real-time PDF processing when new files are added to the directory."""
    
    def __init__(self, chatbot_processor):
        self.chatbot_processor = chatbot_processor

    def on_created(self, event):
        """Triggers when a new PDF is added."""
        if event.src_path.endswith(".pdf"):
            print(f"New PDF detected: {event.src_path}")
            success = self.chatbot_processor.process_pdf(event.src_path)
            if success:
                print(f"Processed: {event.src_path}")

class ChatbotProcessor:
    """Handles PDF processing and Q&A retrieval without external AI models."""
    
    _instance = None
    DEFAULT_PDF_DIRECTORY = "Documentation"

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        """Initialize chatbot processor and auto-load existing PDFs."""
        # Applied SBERT API here
        self.sbert_model = SentenceTransformer("all-MiniLM-L6-v2")
        self.question_embeddings = []

        self.stored_questions = []
        self.qa_pairs = {}
        self.pdf_content = ""
        
        self.load_existing_pdfs()  # Load PDFs on startup
        self.start_folder_monitoring()  # Start monitoring in background

    def load_existing_pdfs(self):
        """Loads PDFs from the defined directory."""
        if os.path.exists(self.DEFAULT_PDF_DIRECTORY):
            for filename in os.listdir(self.DEFAULT_PDF_DIRECTORY):
                if filename.endswith(".pdf"):
                    self.process_pdf(os.path.join(self.DEFAULT_PDF_DIRECTORY, filename))

    def get_embedding(self, text):
        return self.sbert_model.encode(text, convert_to_tensor=True)

    def process_pdf(self, file_path: str) -> bool:
        """Extracts questions and answers from a PDF and appends them to the database."""
        try:
            with pdfplumber.open(file_path) as pdf:
                pdf_text = "\n".join(page.extract_text() for page in pdf.pages if page.extract_text())

            lines = pdf_text.split("\n")
            current_question = None
            current_answer = []

            for line in lines:
                stripped_line = line.strip()
                if stripped_line.endswith("?"):
                    if current_question:
                        self.qa_pairs[current_question.lower()] = "\n".join(current_answer).strip()
                    current_question = stripped_line
                    current_answer = []
                    self.stored_questions.append(current_question)
                    self.question_embeddings.append(self.get_embedding(current_question))
                    embedding = self.get_embedding(current_question)
                    print(f"[DEBUG] Question: {current_question} | Embedding shape: {embedding.shape}")
                elif current_question:
                    current_answer.append(stripped_line)

            if current_question:
                self.qa_pairs[current_question.lower()] = "\n".join(current_answer).strip()

            return True
        except Exception as e:
            print(f"Error processing PDF: {e}")
            return False
    
    def find_best_match(self, user_input):
        input_emb = self.get_embedding(user_input)

        if not self.question_embeddings:
            return None

        question_tensor = torch.stack(self.question_embeddings)
        scores = util.cos_sim(input_emb, question_tensor)
        scores_flat = scores.squeeze(0)

        max_score, best_idx = torch.max(scores_flat, dim=0)
        max_score_value = max_score.item()                         #Extract scalar

        print(f"[DEBUG] Max similarity score: {max_score_value}")
        if max_score_value > 0.75:
            return self.stored_questions[best_idx]
        return None

    def chat(self, message: str) -> dict:
        """Matches user queries with stored Q&A pairs."""
        try:
            greetings = ["hi", "hello", "hey", "hola", "namaste"]
            well_being_questions = ["how are you", "how are you?", "how's it going", "how do you do"]

            if message.lower() in greetings:
                return {"response": "Greetings! How can I assist you today?", "error": None}

            if message.lower() in well_being_questions:
                return {"response": "I am good and ready to help! How can I assist you?", "error": None}
            
            matched_question = self.find_best_match(message)
            # print(f"[DEBUG] QA Pairs Keys: {list(self.qa_pairs.keys())[:5]}") 
            if matched_question:
                key = matched_question.lower().strip()
                print(f"[DEBUG] Matched question: {key}")
                answer = self.qa_pairs.get(key)
                if answer:
                    return {"response": answer.replace("\n", " "), "error": None}
                else:
                    return {"response": "Hmm, I matched your question but couldn't retrieve the answer.", "error": True}
            else:
                return {"response": "Trouble finding what you're looking for. Try rephrasing your question.", "error": True}
        except Exception as e:
            return {"response": str(e), "error": True}
    
    def get_chat_history(self):
        """Retrieves stored questions."""
        return self.stored_questions
    
    def clear_history(self):
        """Clear stored conversation history"""
        self.history = []

    def start_folder_monitoring(self):
        """Starts a background thread to monitor the directory for new PDFs."""
        event_handler = PDFHandler(self)
        observer = Observer()
        observer.schedule(event_handler, path=self.DEFAULT_PDF_DIRECTORY, recursive=False)
        observer_thread = threading.Thread(target=self._run_observer, args=(observer,))
        observer_thread.daemon = True  # Runs in background
        observer_thread.start()
        print(f"Monitoring '{self.DEFAULT_PDF_DIRECTORY}' for new PDFs in the background...")

    def _run_observer(self, observer):
        """Runs the observer loop in a background thread."""
        observer.start()
        try:
            while True:
                time.sleep(5)  # Keeps running in the background
        except KeyboardInterrupt:
            observer.stop()
        observer.join()
