import streamlit as st
from src import dashboard, input_fields


def main():
    tab1, tab2 = st.tabs(["Dashboard", "Predict"])

    with tab1:
        dashboard()

    with tab2:
        input_fields()

if __name__ == "__main__":
    main()