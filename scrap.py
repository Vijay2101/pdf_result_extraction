import streamlit as st
import fitz  # PyMuPDF
from result import format1
from result2 import format2
import json

def main():
    st.title('PDF Format Converter')

    # Upload PDF
    uploaded_file = st.file_uploader("Choose a PDF file", type="pdf")
    
    if uploaded_file is not None:
        # Display PDF upload status
        st.write("File uploaded successfully!")

        # Format selection
        format_option = st.selectbox("Choose the format", ["Format1", "Format2"])

        # Submit button
        if st.button("Submit"):
            # Here you can add the code to process the PDF based on the selected format
            if format_option == 'Format1':
                res = format1(uploaded_file)
            if format_option =='Format2':
                res = format2(uploaded_file)
            st.json(res)
            st.success(f"PDF processed in {format_option} format!")



if __name__ == "__main__":
    main()
