import pytest
from fastapi.testclient import TestClient
from ingestion_service.main import app
import pandas as pd
from pathlib import Path

client = TestClient(app)

def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

def test_upload_file_invalid_format():
    response = client.post(
        "/api/v1/upload",
        files={"file": ("test.txt", b"test content", "text/plain")}
    )
    
    assert response.status_code == 400
    assert "Invalid file format" in response.json()["detail"]

@pytest.fixture
def test_excel_file(tmp_path):
    file_path = tmp_path / "test.xlsx"
    df = pd.DataFrame({
        'Номер сертификата': ['TEST-123'],
        'Статус': ['Действует'],
        'Тип заявителя': ['Юридическое лицо']
    })
    df.to_excel(file_path, index=False)
    return file_path

def test_upload_file_success(test_excel_file):
    with open(test_excel_file, "rb") as f:
        response = client.post(
            "/api/v1/upload",
            files={"file": ("test.xlsx", f, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
        )
    
    assert response.status_code == 200
    data = response.json()
    assert "file_id" in data

def test_get_processing_status():
    # Сначала загружаем файл
    test_file = Path(__file__).parent / "test_data" / "test.xlsx"
    if not test_file.exists():
        pytest.skip("Test file not found")
    
    with open(test_file, "rb") as f:
        response = client.post(
            "/api/v1/upload",
            files={"file": ("test.xlsx", f, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
        )
    
    file_id = response.json()["file_id"]
    
    # Проверяем статус обработки
    response = client.get(f"/api/v1/status/{file_id}")
    assert response.status_code == 200
    data = response.json()
    assert "status" in data