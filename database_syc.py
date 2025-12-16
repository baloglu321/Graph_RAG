from llama_index.core import Document, PropertyGraphIndex, Settings
from llama_index.graph_stores.neo4j import Neo4jPropertyGraphStore
from llama_index.llms.ollama import Ollama
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.core.node_parser import JSONNodeParser
from llama_index.core.node_parser import SentenceSplitter
from tqdm import tqdm
import os
import hashlib
import nest_asyncio
import json

nest_asyncio.apply()

CLOUDFLARE_TUNNEL_URL = ".../"
OLLAMA_MODEL_ID = "gemma3:27b"
# ---------------------------------------------------------
# 1. AYARLAR
# ---------------------------------------------------------
JSON_DIR = "./database"  # JSON dosyalarının olduğu klasör
STATE_FILE = "file_state.json"  # Hangi dosyanın işlendiğini tutan hafıza dosyası

Settings.llm = Ollama(
    model=OLLAMA_MODEL_ID,
    base_url=CLOUDFLARE_TUNNEL_URL,
    context_window=8192,
    # verbose=True ekleyerek LiteLLM'in HTTP isteklerini gorebilirsiniz.
    request_timeout="3000",
)

Settings.embed_model = HuggingFaceEmbedding(
    model_name="paraphrase-multilingual-mpnet-base-v2"
)

# 2. Neo4j Bağlantısı
# Şifreni az önce belirlediğin şifreyle değiştir
graph_store = Neo4jPropertyGraphStore(
    username="neo4j",
    password="neo4j/your_password",
    url="bolt://localhost:7687",
)


# Mevcut Graph İndeksini Yükle (Sıfırdan oluşturmak yerine var olana bağlanır)
index = PropertyGraphIndex.from_existing(
    property_graph_store=graph_store, embed_model=Settings.embed_model, llm=Settings.llm
)

# ---------------------------------------------------------
# 2. YARDIMCI FONKSİYONLAR (HASH & STATE)
# ---------------------------------------------------------


def calculate_file_hash(filepath):
    """Dosya içeriğinin MD5 özetini çıkarır. İçerik değişirse bu özet değişir."""
    hasher = hashlib.md5()
    with open(filepath, "rb") as f:
        buf = f.read()
        hasher.update(buf)
    return hasher.hexdigest()


def load_state():
    """Önceki çalıştırmadan kalan dosya durumlarını yükler."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {}


def save_state(state):
    """Güncel dosya durumlarını kaydeder."""
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=4)


def clean_old_data_from_graph(filename):
    """
    Eğer dosya güncellendiyse, o dosyaya ait ESKİ node'ları graph'tan siler.
    Bunu yapmazsak graph'ta duplicate (kopya) veriler oluşur.
    """
    driver = graph_store._driver
    # Cypher sorgusu: Kaynağı (source) bu dosya olan tüm düğümleri sil
    query = """
    MATCH (n) 
    WHERE n.source_file = $filename 
    DETACH DELETE n
    """
    with driver.session() as session:
        session.run(query, filename=filename)
    print(f"🗑️  Eski veriler silindi: {filename}")


# ---------------------------------------------------------
# 3. ANA MANTIK (GÜNCELLEME KONTROLÜ)
# ---------------------------------------------------------


def process_documents():
    current_state = load_state()
    new_state = current_state.copy()

    # Database klasöründeki tüm .json dosyalarını bul
    files = [f for f in os.listdir(JSON_DIR) if f.endswith(".json")]

    files_processed_count = 0

    for filename in files:
        filepath = os.path.join(JSON_DIR, filename)
        current_hash = calculate_file_hash(filepath)

        # KONTROL: Dosya daha önce işlendi mi ve içeriği aynı mı?
        if filename in current_state and current_state[filename] == current_hash:
            print(f"⏩ Değişiklik yok, atlanıyor: {filename}")
            continue

        print(f"🔄 İşleniyor (Yeni veya Değişmiş): {filename}")

        # 1. Adım: Eğer dosya güncelleniyorsa, Graph'tan eskisini temizle
        if filename in current_state:
            clean_old_data_from_graph(filename)

        # 2. Adım: Dosyayı Oku
        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()  # JSON'u string olarak okuyoruz

        # 3. Adım: Document Objesi Oluştur
        splitter = SentenceSplitter(chunk_size=1024, chunk_overlap=50)
        text_chunks = splitter.split_text(content)

        print(
            f"📄 {filename} dosyası {len(text_chunks)} parçaya bölündü. Graph'a işleniyor..."
        )

        # Her parçayı Document objesine çevir
        documents = [
            Document(text=chunk, metadata={"source_file": filename})
            for chunk in text_chunks
        ]

        # 4. Adım: Graph'a Ekle (Insert) - DÖNGÜ İLE
        # index.insert() tek seferde tek belge alır. Listeyi döngüye sokuyoruz.
        # tqdm sayesinde ekranda [=====>     ] şeklinde ilerleme çubuğu çıkacak.

        for doc in tqdm(documents, desc=f"🚀 {filename} işleniyor", unit="chunk"):
            try:
                index.insert(doc)
            except Exception as e:
                print(f"\n⚠️ Hata (Bu parça atlandı): {e}")
                # Hata olsa bile döngü devam etsin, tüm işlem durmasın.
                continue

        # 5. Adım: State'i güncelle
        new_state[filename] = current_hash
        save_state(new_state)
        files_processed_count += 1

    print(f"\n✅ İşlem tamamlandı. Toplam güncellenen dosya: {files_processed_count}")


# ---------------------------------------------------------
# 4. ÇALIŞTIR VE TEST ET
# ---------------------------------------------------------
if __name__ == "__main__":
    # Klasör yoksa uyar
    if not os.path.exists(JSON_DIR):
        os.makedirs(JSON_DIR)
        print(
            f"📁 '{JSON_DIR}' klasörü oluşturuldu. Lütfen içine JSON dosyalarını koy."
        )
    else:
        process_documents()

        # Test sorusu (İsteğe bağlı)
        # query_engine = index.as_query_engine(include_text=True)
        # print(query_engine.query("Veritabanındaki son bilgiler ışığında özet geç."))
