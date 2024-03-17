import streamlit as st
import pandas as pd

def dashboard():
    # Sample data
    data = {
        'Week': ['Week 1', 'Week 2', 'Week 3', 'Week 4', 'Week 5'],
        'Black Panther (2018)': [242155680, 403613257, 501706972, 561697180, 605027218],
        'Black Panthers: Vanguard of the Revolution (2015)': [27245, 86167, 151282, 219446, 418408]
    }

    df = pd.DataFrame(data)
    df.set_index('Week', inplace=True)

    # Streamlit app
    st.title('Movie Performance Dashboard')

    if st.button("Predict my movie performance", key = "go_to_input_field"):
        print("Predict button clicked")
        st.session_state.page = "Input Fields"

    st.write('### Box Office Performance Over Weeks')
    st.line_chart(df)

    st.write('### Ranking Over Weeks')
    st.area_chart(df)