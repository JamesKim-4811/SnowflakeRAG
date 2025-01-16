import os
from dotenv import load_dotenv
import pprint

import streamlit as st # Import python packages
from snowflake.snowpark.context import get_active_session

from snowflake.cortex import Complete
from snowflake.core import Root

import pandas as pd
import json

pd.set_option("max_colwidth",None)

from snowflake.snowpark import Session

load_dotenv()

### Default Values
NUM_CHUNKS = 10 # Num-chunks provided as context. Play with this to check how it affects your accuracy
slide_window = 7 # how many last conversations to remember. This is the slide window.

# service parameters
CORTEX_SEARCH_DATABASE = "SEMANTIC_SEARCH_DB"
CORTEX_SEARCH_SCHEMA = "DOCS_SCHEMA"
CORTEX_SEARCH_SERVICE = "SEMANTIC_CORTEX_SEARCH_SERVICE"
######
######

# columns to query in the service
COLUMNS = [
    "doc_text",
    "relative_path",
    "subject"
]

@st.cache_resource
def get_snowpark_session():
    connection_parameters = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_USER_PASSWORD"],
        "role": "ACCOUNTADMIN",
        "database": "SEMANTIC_SEARCH_DB",
        "warehouse": "SEMANTIC_WH",
        "schema": "DOCS_SCHEMA",
    }
    return Session.builder.configs(connection_parameters).create()

# Use the cached session
session = get_snowpark_session()
root = Root(session)                    

svc = root.databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[CORTEX_SEARCH_SERVICE]
   
### Functions
     
def config_options():

    st.session_state.model_name = 'mistral-large2'

    st.session_state.use_docs = True

    st.session_state.use_chat_history = True
    st.sidebar.checkbox('Debug: Click to see summary generated of previous conversation', key="debug", value = True)
    st.sidebar.button("Start Over", key="clear_conversation", on_click=init_messages)
    st.sidebar.expander("Session State").write(st.session_state)
    
def init_messages():

    # Initialize chat history
    if st.session_state.clear_conversation or "messages" not in st.session_state:
        st.session_state.messages = []

def get_similar_chunks_search_service(query):

    filters = {}
    
    # Filter by subject if a subject is selected (not "ALL")
    if st.session_state.subject_value:
        filters["@eq"] = {"subject": st.session_state.subject_value}
    
    # Filter by grade if a grade is selected
    if st.session_state.grade_value:
        filters["@eq"] = {"grade": st.session_state.grade_value}
    
    # Combine both filters (subject and grade) if both are present
    if filters:
        response = svc.search(query, COLUMNS, filter=filters, limit=NUM_CHUNKS)
    else:
        response = svc.search(query, COLUMNS, limit=NUM_CHUNKS)
    
    with st.sidebar.expander("chunks"):
        st.json(response.model_dump_json())
    
    return response.model_dump_json()  

def get_chat_history():
#Get the history from the st.session_stage.messages according to the slide window parameter
    
    chat_history = []
    
    start_index = max(0, len(st.session_state.messages) - slide_window)
    for i in range (start_index , len(st.session_state.messages) -1):
         chat_history.append(st.session_state.messages[i])

    return chat_history

def summarize_question_with_history(chat_history, question):
# To get the right context, use the LLM to first summarize the previous conversation
# This will be used to get embeddings and find similar chunks in the docs for context

    prompt = f"""
        Based on the chat history below and the question, generate a query that extend the question
        with the chat history provided. The query should be in natual language. 
        Answer with only the query. Do not add any explanation.
        
        <chat_history>
        {chat_history}
        </chat_history>
        <question>
        {question}
        </question>
        """
    
    sumary = Complete(st.session_state.model_name, prompt, session=session)   

    if st.session_state.debug:
        st.sidebar.expander("Summary to be used to find similar chunks in the docs:").write(sumary)

    sumary = sumary.replace("'", "")

    return sumary

def create_prompt (myquestion):

    if st.session_state.use_docs:
        if st.session_state.use_chat_history:
            chat_history = get_chat_history()

            if chat_history != []: #There is chat_history, so not first question
                question_summary = summarize_question_with_history(chat_history, myquestion)
                prompt_context =  get_similar_chunks_search_service(question_summary)
            else:
                prompt_context = get_similar_chunks_search_service(myquestion) #First question when using history
        else:
            prompt_context = get_similar_chunks_search_service(myquestion)
            chat_history = ""
  
        prompt = f"""
            You are an expert chat assistance that extracs information from the CONTEXT provided
            between <context> and </context> tags.
            You offer a chat experience considering the information included in the CHAT HISTORY
            provided between <chat_history> and </chat_history> tags..
            When ansering the question contained between <question> and </question> tags
            be concise and do not hallucinate. 
            If you don´t have the information just say so.
            
            Do not mention the CONTEXT used in your answer.
            Do not mention the CHAT HISTORY used in your asnwer.

            Only anwer the question if you can extract it from the CONTEXT provideed.
            
            <chat_history>
            {chat_history}
            </chat_history>
            <context>          
            {prompt_context}
            </context>
            <question>  
            {myquestion}
            </question>
            Answer: 
            """
        
        json_data = json.loads(prompt_context)

        relative_paths = set(item['relative_path'] for item in json_data['results'])

    else:     
        if st.session_state.use_chat_history:
            chat_history = get_chat_history()

            if chat_history != []: #There is chat_history, so not first question
                question_summary = summarize_question_with_history(chat_history, myquestion)
                prompt_context =  get_similar_chunks_search_service(question_summary)
            else:
                prompt_context = get_similar_chunks_search_service(myquestion) #First question when using history
        else:
            prompt_context = get_similar_chunks_search_service(myquestion)
            chat_history = ""
        prompt = f"""
            You offer a chat experience considering the information included in the CHAT HISTORY
            provided between <chat_history> and </chat_history> tags..
            When ansering the question contained between <question> and </question> tags
            be concise. 
            
            Do not mention the CHAT HISTORY used in your asnwer.

            <chat_history>
            {chat_history}
            </chat_history>
            <question>  
            {myquestion}
            </question>
            Answer: 
            """
        relative_paths = "None"

    return prompt, relative_paths


def answer_question(myquestion):

    prompt, relative_paths =create_prompt (myquestion)

    response = Complete(st.session_state.model_name, prompt, session=session)   

    return response, relative_paths

def generate_quiz():
    subject = st.session_state.subject_value
    grade = st.session_state.grade_value
    
    # Construct the prompt to generate a quiz
    prompt = f"""
    Generate a set of 5 quiz questions for {subject} at {grade} level.
    Include the questions along with possible answer options. 
    Provide the correct answer at the end of each question.
    """

    # Send the prompt to the model (Cortex)
    with st.chat_message("assistant"):
            message_placeholder = st.empty()

            with st.spinner(f"{st.session_state.model_name} thinking..."):
                response, relative_paths = answer_question(prompt)            
                response = response.replace("'", "")

                with st.sidebar.expander("Related Documents"):
                    for path in relative_paths:
                        cmd2 = f"""
                        SELECT DISTINCT FILE_URL 
                        FROM table('education_docs') 
                        WHERE relative_path = '{path}' 
                        """
                        df_url_link = session.sql(cmd2).to_pandas()
                        url_link = df_url_link._get_value(0,'FILE_URL')
                        display_url = f"Doc: [{path}]({url_link})"
                        
                        st.write(display_url)
    st.session_state.messages.append({"role": "assistant", "content": response})

    # Display the generated quiz
    st.subheader(f"Generated Quiz for {subject} ({grade})")
    st.markdown(response)
    
def main():
    
    st.title(f":speech_balloon: Quiz Generator with Snowflake Cortex")

    grades = session.table('education_docs').select('grade').distinct().collect()
    grades_list = ['ALL']
    for grade in grades:
        grades_list.append(grade.GRADE)        
    st.selectbox('Select a grade', grades_list, key = "grade_value")

    subjects = session.table('education_docs').select('subject').distinct().collect()
    sub_list = ['ALL']
    for sub in subjects:
        sub_list.append(sub.SUBJECT)
    st.selectbox('Select a subject to test', sub_list, key = "subject_value")

    st.button("Generate Quiz", on_click=generate_quiz)

    config_options()
    # st.sidebar.button("Generate Quiz", on_click=generate_quiz)
    init_messages()
    
   
                            

        


if __name__ == "__main__":
    main()