from Webapp.models.words import get_everything_from_words
import json

def export_words_to_excel(filename="words.xlsx"):
    rows = get_everything_from_words()
    
    import openpyxl
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Words"

    if len(rows) > 0:
        if isinstance(rows[0], dict):
            headers = list(rows[0].keys())
        else:
            headers = rows[0].keys()
        
        ws.append(headers)

        for row in rows:
            values = []
            for h in headers:
                val = row[h]
                if isinstance(val, (list, dict)):
                    # 转成 JSON 字符串保存
                    val = json.dumps(val, ensure_ascii=False)
                values.append(val)
            ws.append(values)
    else:
        ws.append(["No data found"])

    wb.save(filename)
    print(f"✅ 导出成功：{filename}")

if __name__ == "__main__":
    export_words_to_excel()