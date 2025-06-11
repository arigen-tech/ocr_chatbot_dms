import os
import sqlite3
import threading
import subprocess
import pdfplumber
import pytesseract
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pathlib import Path
from docx import Document
from pyzbar.pyzbar import decode
from PIL import Image
import pandas as pd
import openpyxl

class DocumentHandler(FileSystemEventHandler):
    """Handler for document file system events."""
    def __init__(self, document_processor):
        self.document_processor = document_processor
        self.image_extensions = ['.png', '.jpg', '.jpeg', '.bmp', '.gif', '.tiff', '.webp']
        self.word_extensions = ['.docx', '.doc', '.docm', '.dotx', '.dotm']
        self.excel_extensions = ['.xlsx', '.xls', '.xlsm', '.csv', '.xlsb']
        self.valid_extensions = ['.pdf', '.txt'] + self.image_extensions + self.word_extensions + self.excel_extensions

    def on_created(self, event):
        file_path = event.src_path
        
        if not any(file_path.endswith(ext) for ext in self.valid_extensions):
            return
            
        print(f"\nNew document detected: {file_path}")
        self.document_processor.process_single_document(file_path)

class DocumentProcessor:
    _instance = None
    _lock = threading.Lock()
    
    @classmethod
    def get_instance(cls, base_dir="PDFs_Data", db_name="search-docs.db"):
        """Singleton pattern to ensure only one instance exists and loads existing files."""
        if not cls._instance:
            cls._instance = cls(base_dir, db_name)
            cls._instance.load_existing_documents()
        return cls._instance

    def __init__(self, base_dir, db_name):
        if not base_dir or not db_name:
            raise ValueError("base_dir and db_path are required")
            
        self.base_dir = Path(base_dir)
        self.db_path = self.base_dir / db_name
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._initialize_db()

    def _initialize_db(self):
        """Create the db file inside base_dir if it doesn't exist or recreate to include missing columns."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Check if the table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='document_data';")
        table_exists = cursor.fetchone()

        if table_exists:
            # Fetch existing data to migrate
            cursor.execute("SELECT file_name, content FROM document_data;")
            existing_data = cursor.fetchall()

            # Drop the old virtual table
            cursor.execute("DROP TABLE document_data;")
            conn.commit()
        
        # Create the new virtual table with the qr_data column
        cursor.execute("""
            CREATE VIRTUAL TABLE document_data USING FTS5(
                file_name, content, qr_data
            );
        """)
        conn.commit()

        # Restore the old data into the newly created table
        if table_exists:
            cursor.executemany("INSERT INTO document_data (file_name, content) VALUES (?, ?);", existing_data)
            conn.commit()

        conn.close()

    def get_db_connection(self):
        """Create a new database connection for the current thread."""
        return sqlite3.connect(self.db_path)

    def load_existing_documents(self):
        """Scan all subdirectories for documents and add them to the database dynamically."""
        image_extensions = ['.png', '.jpg', '.jpeg', '.bmp', '.gif', '.tiff', '.webp']
        word_extensions = ['.docx', '.doc', '.docm', '.dotx', 'dotm']
        excel_extensions = ['.xlsx', '.xls', '.xlsm', '.csv', '.xlsb']
        valid_extensions = ['.pdf', '.txt'] + image_extensions + word_extensions + excel_extensions        
        doc_files = [f for f in self.base_dir.rglob("*") if f.suffix in valid_extensions]

        if not doc_files:
            print(f"No Documents found in {self.base_dir}")
            return

        print(f"Loading {len(doc_files)} Documents into the database...")

        for doc_path in doc_files:
            self.process_single_document(str(doc_path))

        print("Database initialization complete.")

    def extract_text_from_pdf(self, doc_path):
        """Extract text from PDF files."""
        full_text = ""
        qr_content = set()

        try:
            with pdfplumber.open(doc_path) as pdf:
                for page in pdf.pages:
                    text = page.extract_text(x_tolerance=2)
                    if text:
                        full_text += text + "\n"
                    
                    img_pil = page.to_image(resolution=300).original
                    ocr_text = pytesseract.image_to_string(img_pil)
                    full_text += ocr_text + "\n"

                    qr_codes = decode(img_pil)
                    for qr in qr_codes:
                        qr_content.add(qr.data.decode())

            return full_text.strip(), "; ".join(qr_content) if qr_content else None
        
        except Exception as e:
            print(f"Error extracting from PDF {doc_path}: {str(e)}")
            return None, None
        
    def convert_to_docx(self, input_path):
        """Convert .doc, .docm, .dotx, .dotm to .docx using unoconv."""
        output_path = os.path.splitext(input_path)[0] + ".docx"
        try:
            subprocess.run(["unoconv", "-f", "docx", "-o", output_path, input_path], check=True)

            if os.path.exists(output_path):
                return output_path
            else:
                print(f"Conversion failed: {input_path} → {output_path}")
                return None
        except subprocess.CalledProcessError as e:
            print(f"Error converting {input_path} to .docx: {str(e)}")
            return None    
    
    def extract_text_from_word(self, doc_path):
        """Extract text from various Word formats."""
        ext = os.path.splitext(doc_path)[1].lower()
        if ext in ['.doc', '.docm', '.dotx', '.dotm']:
            converted_path = self.convert_to_docx(doc_path)
            if not converted_path:
                return None
            doc_path = converted_path

        if not os.path.exists(doc_path):
            print(f"File does not exist: {doc_path}")
            return None

        try:
            doc = Document(doc_path)
            return "\n".join([para.text for para in doc.paragraphs])
        except Exception as e:
            print(f"Error extracting text from {doc_path}: {str(e)}")
            return None
    
    def extract_text_from_txt(self, txt_path):
        """Extract text from .txt files."""
        try:
            with open(txt_path, 'r', encoding='utf-8') as file:
                return file.read()
        except Exception as e:
            print(f"Error extracting from TXT {txt_path}: {str(e)}")
            return None
    
    def extract_text_from_image(self, img_path):
        """Extract text and QR codes from images, supporting all common formats."""
        try:
            img_pil = Image.open(img_path)
            text = pytesseract.image_to_string(img_pil)
            qr_codes = decode(img_pil)

            qr_content = "; ".join(qr.data.decode() for qr in qr_codes) if qr_codes else None
            return text.strip(), qr_content
        except Exception as e:
            print(f"Error extracting from Image {img_path}: {str(e)}")
            return None, None
        
    def extract_text_from_excel(self, excel_path):
        """Extract text from Excel files, supporting all standard formats."""
        try:
            if excel_path.endswith('.csv'):
                # Handle CSV using pandas
                df = pd.read_csv(excel_path)
                return df.to_string()

            elif excel_path.endswith(('.xls', '.xlsx', '.xlsm', '.xlsb')):
                # Handle Excel files using openpyxl for better compatibility
                wb = openpyxl.load_workbook(excel_path, data_only=True)
                sheets_text = []

                for sheet in wb.worksheets:
                    for row in sheet.iter_rows(values_only=True):
                        sheets_text.append(" ".join([str(cell) for cell in row if cell]))

                return "\n".join(sheets_text)

            else:
                return None  # Unsupported format
        except Exception as e:
            print(f"Error extracting from Excel {excel_path}: {str(e)}")
            return None

    def is_file_in_database(self, conn, file_name):
        """Check if a file exists in the database."""
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM document_data WHERE file_name = ?", (file_name,))
        return cursor.fetchone() is not None
    
    def process_queue(self):
        """Process Documents from the queue continuously using parallel processing."""
        while True:
            try:
                doc_path = self.processing_queue.get()
                self.executor.submit(self.process_single_document, doc_path)  # Async processing
                self.processing_queue.task_done()
            except Exception as e:
                print(f"Error processing document from queue: {e}")

    def process_single_document(self, doc_path):
        """Process a document while ensuring extensions align with DocumentHandler."""
        conn = self.get_db_connection()
        try:
            doc_path = Path(doc_path)
            file_name = doc_path.name

            handler = DocumentHandler(self)  # Instantiate the handler to access valid extensions

            if doc_path.suffix.lower() not in handler.valid_extensions:
                print(f"Skipping unsupported file type: {file_name}")
                return

            print(f"Processing and inserting: {file_name}")

            text, qr_data = None, None

            if doc_path.suffix.lower() in handler.image_extensions:
                text, qr_data = self.extract_text_from_image(doc_path)
            elif doc_path.suffix.lower() in handler.word_extensions:
                text = self.extract_text_from_word(doc_path)
            elif doc_path.suffix.lower() in handler.excel_extensions:
                text = self.extract_text_from_excel(doc_path)
            elif doc_path.suffix.lower() == ".pdf":
                text, qr_data = self.extract_text_from_pdf(doc_path)
            elif doc_path.suffix.lower() == ".txt":
                text = self.extract_text_from_txt(doc_path)

            if text:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO document_data (file_name, content, qr_data) VALUES (?, ?, ?);",
                    (file_name, text, qr_data)
                )
                conn.commit()
                print(f"Successfully stored: {file_name}")
            
        finally:
            conn.close()

    def get_all_documents(self):
        """Retrieve all Documents names stored in the database."""
        conn = self.get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT file_name FROM document_data")
            return [row[0] for row in cursor.fetchall()]
        finally:
            conn.close()

    def search_database(self, query, selected_files=None):
        """Search the database for documents containing the specified query."""
        conn = self.get_db_connection()
        try:
            cursor = conn.cursor()
            if selected_files:
                placeholders = ','.join('?' * len(selected_files))
                cursor.execute(f"""
                    SELECT DISTINCT file_name 
                    FROM document_data 
                    WHERE content MATCH ? 
                    AND file_name IN ({placeholders})
                """, (query, *selected_files))
            else:
                cursor.execute("SELECT DISTINCT file_name FROM document_data WHERE content MATCH ?", (query,))
            return [row[0] for row in cursor.fetchall()]
        finally:
            conn.close()
    
    def clean_database(self):
        """Delete all entries in the document_data table, effectively resetting the database."""
        conn = self.get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM document_data")  # Remove all records
            conn.commit()
            print("Database cleaned successfully.")
        except sqlite3.Error as e:
            print(f"Database error while cleaning: {str(e)}")
        finally:
            conn.close()

    def start_processing(self):
        """Start the document processing service."""
        self.load_existing_documents()

        event_handler = DocumentHandler(self)
        observer = Observer()
        observer.schedule(event_handler, str(self.base_dir), recursive=True)
        observer.start()

        return observer