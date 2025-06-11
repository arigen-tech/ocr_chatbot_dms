import os
import pdfplumber
from difflib import get_close_matches

class ChatbotProcessor:
    """Handles PDF processing and Q&A retrieval without external AI models."""

    _instance = None
    DEFAULT_PDF_DIRECTORY = "Documentation"  # Path where PDFs are stored

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        """Initialize the chatbot processor and auto-load predefined PDFs."""
        self.stored_questions = []
        self.qa_pairs = {}
        self.pdf_content = ""

        # Auto-load PDFs on startup
        self.load_existing_pdfs()

    def load_existing_pdfs(self):
        """Automatically loads PDFs from the defined directory on startup."""
        if os.path.exists(self.DEFAULT_PDF_DIRECTORY):
            for filename in os.listdir(self.DEFAULT_PDF_DIRECTORY):
                if filename.endswith(".pdf"):
                    self.process_pdf(os.path.join(self.DEFAULT_PDF_DIRECTORY, filename))

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
                    if current_question:  # Store previous Q&A pair
                        self.qa_pairs[current_question.lower()] = "\n".join(current_answer).strip()
                    current_question = stripped_line
                    current_answer = []
                    self.stored_questions.append(current_question)
                elif current_question:  # Append lines to answer until next question starts
                    current_answer.append(stripped_line)

            # Store last Q&A pair
            if current_question:
                self.qa_pairs[current_question.lower()] = "\n".join(current_answer).strip()

            return True
        except Exception as e:
            print(f"Error processing PDF: {e}")
            return False

    def chat(self, message: str) -> dict:
        """Matches user queries with PDF content and provides stored answers."""
        try:
            # First-time message handling
            greetings = ["hi", "hello", "hey", "hola", "namaste"]
            well_being_questions = ["how are you", "how are you?", "how's it going", "how do you do"]

            if message.lower() in greetings:
                return {"response": "Hello! How can I assist you today?", "error": None}

            if message.lower() in well_being_questions:
                return {"response": "I am good and ready to help! How can I assist you?", "error": None}

            closest_match = get_close_matches(message.lower(), self.qa_pairs.keys(), n=1, cutoff=0.6)

            if closest_match:
                matched_question = closest_match[0]
                answer = self.qa_pairs.get(matched_question, "No answer found").replace("\n", " ")
                return {"response": answer, "error": None}
            else:
                return {"response": "I cannot help you with this, please ask questions related to the application.", "error": True}
        except Exception as e:
            return {"response": str(e), "error": True}

    def clear_history(self):
        """Clears stored questions and PDF content."""
        self.stored_questions = []
        self.qa_pairs = {}
        self.pdf_content = ""

    def get_chat_history(self):
        """Retrieves stored questions."""
        return self.stored_questions