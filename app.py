import psycopg2
import streamlit as st
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from openai import OpenAI


# Database Connection Helper
def save_feedback(user_query: str, llm_response: str, rating: int):
    try:
        conn = psycopg2.connect(
            dbname="feedback_db",
            user="airflow",
            password="airflow",
            host="postgres",  # Docker service name
            port="5432"
        )
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO user_feedback (user_query, llm_response, rating)
            VALUES (%s, %s, %s);
            """,
            (user_query, llm_response, rating)
        )
        conn.commit()
        cur.close()
        conn.close()
        st.toast("Feedback saved! Thank you.", icon="✅")
    except Exception as e:
        st.error(f"Failed to record feedback: {e}")


# Page configuration
st.set_page_config(
    page_title="EU AI Act Compliance Assistant", 
    page_icon="⚖️",
    layout="centered", 
    initial_sidebar_state="auto"
)
st.title("⚖️ EU AI Act Compliance & Regulation Assistant")

# 1. Cache resources so models and clients don't reload on every click
@st.cache_resource
def load_embedding_model():
    return SentenceTransformer("all-MiniLM-L6-v2")

@st.cache_resource
def get_qdrant_client():
    qdrant_client_url = "http://qdrant:6333"
    return QdrantClient(url=qdrant_client_url)

model = load_embedding_model()
qdrant_client = get_qdrant_client()
collection_name = "eu_ai_act_definitions"

# Initialize OpenAI client (or swap with your preferred LLM provider)
# Make sure your OPENAI_API_KEY environment variable is set
llm_client = OpenAI()

# 2. Maintain chat history in Streamlit session state
if "messages" not in st.session_state:
    st.session_state.messages = []

# 2. Render Chat History + Feedback Widgets
for idx, message in enumerate(st.session_state.messages):
    role = message["role"]
    avatar_icon = "👤" if role == 'user' else "🤖" 
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        
        # Display feedback widget only for assistant messages
        if message["role"] == "assistant":
            feedback_key = f"feedback_{idx}"
            
            # Check if feedback was already submitted for this response
            existing_rating = message.get("feedback")
            
            rating = st.feedback(
                "thumbs", 
                key=feedback_key, 
                disabled=existing_rating is not None
            )
            
            # If user interacts with the feedback widget
            if rating is not None and existing_rating is None:
                # Retrieve the corresponding user query (previous message)
                user_query = st.session_state.messages[idx - 1]["content"]
                llm_response = message["content"]
                
                # Save to PostgreSQL and state
                save_feedback(user_query, llm_response, rating)
                st.session_state.messages[idx]["feedback"] = rating
                st.rerun()

# Display prior chat history
# for message in st.session_state.messages:
#     with st.chat_message(message["role"]):
#         st.markdown(message["content"])

# 3. Handle user input
if user_query := st.chat_input("Ask a question about the EU AI Act definitions..."):
    # Append and display user message
    st.session_state.messages.append({"role": "user", "content": user_query})
    with st.chat_message("user", avatar="👤"):
        st.markdown(user_query)

    with st.chat_message("assistant", avatar="🤖"):
        with st.spinner("Searching regulations and generating answer..."):
            
            # Step 1: Vectorize user query
            query_vector = model.encode(user_query).tolist()
            
            # Step 2: Perform Vector Search in Qdrant
            search_results = qdrant_client.query_points(
                collection_name=collection_name,
                query=query_vector,
                limit=3,
                with_payload=True
            )
            
            # Step 3: Extract and format context from retrieved payloads
            retrieved_chunks = []
            sources = set()
            for hit in search_results.points:
                term = hit.payload.get("term")
                defined_terms = hit.payload.get("defined_terms")
                source = hit.payload.get("source")
                chunk_type = hit.payload.get("chunk_type")
                citation_label = hit.payload.get("citation_label")
                retrieved_chunks.append(f"Term: {term} (Defined Terms: {defined_terms}, Source: {source}, Chunk Type: {chunk_type}, Citation: {citation_label})")
                sources.add(source)
                
            context_text = "\n".join(retrieved_chunks)
            
            # Step 4: Construct the prompt (Context + Question)
            system_prompt = (
                "You are an expert legal compliance assistant specializing in the EU AI Act. "
                "Answer the user's question accurately using *only* the provided context definitions below.\n\n"
                f"Context:\n{context_text}"
            )
            
            # Step 5: Send to LLM
            response = llm_client.chat.completions.create(
                model="gpt-5.4-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_query}
                ],
                temperature=0.1
            )
            
            answer = response.choices[0].message.content
            
            # Append sources used for transparency
            if sources:
                source_str = " | ".join(sources)
                answer += f"\n\n   📄**Sources:** {source_str}"

        # Display assistant response
        st.markdown(answer)
        st.session_state.messages.append({"role": "assistant", "content": answer})
    
    # rerun to display the feedback widget for the new assistant message
    st.rerun()