#!/usr/bin/env python
"""
Document Scaler - Example Usage Script
Demonstrates different ways to use the DocumentScaler service
"""

import os
from pathlib import Path
from Models.document_scaler import DocumentScaler, scale_document, auto_scale_for_ocr
from PIL import Image

def example_1_simple_upscale():
    """Example 1: Simple upscaling with convenience function"""
    print("\n" + "="*60)
    print("EXAMPLE 1: Simple Upscaling")
    print("="*60)
    
    try:
        # File to scale
        file_path = "/path/to/your/document.pdf"
        
        # Upscale to 150%
        result = scale_document(file_path, scale_type=1)
        
        print(f"✓ Original: {file_path}")
        print(f"✓ Scaled:   {result}")
        print(f"✓ Size: 150% of original")
    
    except FileNotFoundError:
        print("ℹ Example: File not found. This is just a demonstration.")

def example_2_downscale_with_output_dir():
    """Example 2: Downscaling with custom output directory"""
    print("\n" + "="*60)
    print("EXAMPLE 2: Downscaling with Custom Output")
    print("="*60)
    
    try:
        file_path = "/path/to/your/image.jpg"
        output_dir = "/custom/output/directory"
        
        # Downscale to 66%
        result = scale_document(
            file_path=file_path,
            scale_type=0,  # downscale
            output_dir=output_dir
        )
        
        print(f"✓ Original:     {file_path}")
        print(f"✓ Scaled:       {result}")
        print(f"✓ Output Dir:   {output_dir}")
        print(f"✓ Size: 66% of original")
    
    except FileNotFoundError:
        print("ℹ Example: File not found. This is just a demonstration.")

def example_3_class_based_api():
    """Example 3: Class-based API with advanced control"""
    print("\n" + "="*60)
    print("EXAMPLE 3: Class-Based API")
    print("="*60)
    
    try:
        # Initialize scaler with base output directory
        scaler = DocumentScaler(output_base_dir="/scaled_documents_storage")
        
        # Scale multiple files
        files = [
            ("report.pdf", 1),      # upscale PDF
            ("photo.jpg", 0),       # downscale image
            ("document.docx", 1),   # upscale Word doc
        ]
        
        for filename, scale_type in files:
            file_path = f"/path/to/{filename}"
            scale_label = "upscale" if scale_type == 1 else "downscale"
            
            try:
                result = scaler.scale_file(file_path, scale_type)
                print(f"✓ {filename:20} → {scale_label:10} → {Path(result).name}")
            except FileNotFoundError:
                print(f"⚠ {filename:20} → Not found (skipped)")
    
    except Exception as e:
        print(f"Error: {e}")

def example_4_format_validation():
    """Example 4: Validate file formats before scaling"""
    print("\n" + "="*60)
    print("EXAMPLE 4: Format Validation")
    print("="*60)
    
    test_files = [
        "document.pdf",
        "photo.jpg",
        "image.png",
        "report.docx",
        "data.xlsx",        # Not supported
        "script.py",        # Not supported
    ]
    
    print("\nSupported Formats:", DocumentScaler.get_supported_formats())
    print("\nValidation Results:")
    
    for filename in test_files:
        is_supported = DocumentScaler.validate_file(filename)
        status = "✓ Supported" if is_supported else "✗ Not Supported"
        print(f"  {status:20} {filename}")

def example_5_ocr_scaling():
    """Example 5: OCR-optimized adaptive scaling (in-memory)"""
    print("\n" + "="*60)
    print("EXAMPLE 5: OCR-Optimized Adaptive Scaling")
    print("="*60)
    
    try:
        scaler = DocumentScaler()
        
        # Load an image
        image_path = "/path/to/document_page.png"
        img = Image.open(image_path)
        
        print(f"Original size: {img.size}")
        
        # Auto-scale for OCR
        # If width < 1500: scale up
        # If width > 3000: scale down
        # Otherwise: keep original
        
        scaled_img = scaler.auto_scale_for_ocr(
            img,
            min_width=1500,
            max_width=3000
        )
        
        print(f"OCR-Optimized size: {scaled_img.size}")
        print("✓ No file I/O - completely in-memory")
        print("✓ Returns PIL Image object")
        
        # Can use scaled_img directly with OCR
        # result = pytesseract.image_to_string(scaled_img)
    
    except FileNotFoundError:
        print("ℹ Example: File not found. This is just a demonstration.")
    except Exception as e:
        print(f"Error: {e}")

def example_6_batch_processing():
    """Example 6: Batch processing multiple files"""
    print("\n" + "="*60)
    print("EXAMPLE 6: Batch Processing")
    print("="*60)
    
    try:
        scaler = DocumentScaler()
        
        # Directory containing files to scale
        source_dir = Path("/path/to/documents")
        supported_files = list(source_dir.glob("*"))
        
        results = {
            "success": [],
            "failed": [],
            "skipped": [],
        }
        
        print(f"Processing {len(supported_files)} files...")
        
        for file_path in supported_files:
            # Skip unsupported formats
            if not DocumentScaler.validate_file(str(file_path)):
                results["skipped"].append(file_path.name)
                continue
            
            try:
                # Upscale all supported files
                scaled_path = scaler.scale_file(str(file_path), scale_type=1)
                results["success"].append(Path(scaled_path).name)
                print(f"✓ {file_path.name}")
            
            except Exception as e:
                results["failed"].append((file_path.name, str(e)))
                print(f"✗ {file_path.name}: {str(e)[:50]}")
        
        # Print summary
        print("\n" + "-"*60)
        print(f"Success:  {len(results['success'])} files")
        print(f"Failed:   {len(results['failed'])} files")
        print(f"Skipped:  {len(results['skipped'])} files (unsupported)")
    
    except Exception as e:
        print(f"Error: {e}")

def example_7_api_client():
    """Example 7: Using via REST API (simulated)"""
    print("\n" + "="*60)
    print("EXAMPLE 7: REST API Usage (Pseudo-code)")
    print("="*60)
    
    print("""
# Using requests library
import requests

# Scale document via API
response = requests.post(
    'http://localhost:8950/scale/document',
    json={
        'file_name': 'report.pdf',
        'scale_type': 1  # upscale
    }
)

if response.status_code == 200:
    data = response.json()
    print(f"Scaled: {data['file_path']}")
    # Use scaled document...
else:
    print(f"Error: {response.json()}")

# Or with httpx (async)
import httpx

async with httpx.AsyncClient() as client:
    response = await client.post(
        'http://localhost:8950/scale/document',
        json={'file_name': 'photo.jpg', 'scale_type': 0}
    )
    print(response.json())
    """)

def example_8_error_handling():
    """Example 8: Proper error handling"""
    print("\n" + "="*60)
    print("EXAMPLE 8: Error Handling")
    print("="*60)
    
    scaler = DocumentScaler()
    
    # Test case 1: Invalid scale type
    print("\n1. Invalid scale_type:")
    try:
        scaler.scale_file("file.pdf", scale_type=5)
    except ValueError as e:
        print(f"   ✓ Caught: {e}")
    
    # Test case 2: Missing file
    print("\n2. Missing file:")
    try:
        scaler.scale_file("/nonexistent/file.pdf", scale_type=1)
    except FileNotFoundError as e:
        print(f"   ✓ Caught: {e}")
    
    # Test case 3: Unsupported format
    print("\n3. Unsupported format:")
    try:
        scaler.scale_file("file.xyz", scale_type=1)
    except ValueError as e:
        print(f"   ✓ Caught: {e}")
    
    print("\n✓ All errors properly handled")

def main():
    """Run all examples"""
    print("\n" + "🚀 DOCUMENT SCALER - USAGE EXAMPLES 🚀".center(60))
    
    examples = [
        ("Simple Upscaling", example_1_simple_upscale),
        ("Downscaling with Custom Output", example_2_downscale_with_output_dir),
        ("Class-Based API", example_3_class_based_api),
        ("Format Validation", example_4_format_validation),
        ("OCR Scaling", example_5_ocr_scaling),
        ("Batch Processing", example_6_batch_processing),
        ("REST API Usage", example_7_api_client),
        ("Error Handling", example_8_error_handling),
    ]
    
    print("\nAvailable Examples:")
    for i, (name, _) in enumerate(examples, 1):
        print(f"  {i}. {name}")
    
    print("\nRunning all examples...\n")
    
    for name, example_func in examples:
        try:
            example_func()
        except Exception as e:
            print(f"✗ Example failed: {e}")
    
    print("\n" + "="*60)
    print("All examples completed!")
    print("="*60 + "\n")

if __name__ == "__main__":
    main()
