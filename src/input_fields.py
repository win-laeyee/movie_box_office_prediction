import streamlit as st

def input_fields():
    st.title("🎩 Input Movie Data To Predict")

    # Text Input
    title = st.text_input("Enter movie title:", "")
    st.write("You entered:", title)

    # Number Input
    runtime = st.number_input("Enter movie runtime:", min_value=0, max_value=300, value=0, step=1)
    st.write("Movie runtime is:", runtime)

    # Dropdown Select
    genre = ["Romance", "Action", "Horror"]
    selected_option = st.selectbox("Select the movie genre:", genre)
    st.write("You selected:", selected_option)

    # Slider Input
    budget = st.slider("Select your movie budget:", min_value=0, max_value=10000000, value=50000, step=1000)
    st.write("Your movie budget is:", budget)
    
    if st.button("Predict", key = "go_to_dashboard"):
        print("Predict button clicked")
        st.session_state['page'] = "Dashboard"
        print("Session state page set to Dashboard")