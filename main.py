import streamlit as st
from src import dashboard, input_fields

# def main():
#     st.sidebar.title("Navigation")
#     page_options = ["Dashboard", "Input Fields"]
#     choice = st.sidebar.radio("Go to", page_options)

#     if choice == "Dashboard":
#         dashboard.dashboard()
#     elif choice == "Input Fields":
#         input_fields.input_fields()

# if __name__ == "__main__":
#     main()

def main():

    # Initialize session state if not already done
    if "page" not in st.session_state:
        st.session_state.page = "Dashboard"  # Set default page to "Dashboard"

    # Handle navigation based on session state
    if st.session_state.page == "Dashboard":
        dashboard.dashboard()
    elif st.session_state.page == "Input Fields":
        input_fields.input_fields()

if __name__ == "__main__":
    main()