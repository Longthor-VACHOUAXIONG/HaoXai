# 🚀 Excel Import Template - Quick Start Guide

## 📁 Files Created

✅ **ViroDB_Import_Template.xlsx** - Complete Excel template with 8 tables  
✅ **EXCEL_IMPORT_TEMPLATE_GUIDE.md** - Detailed documentation  
✅ **create_excel_template.py** - Template generator script  

---

## 🎯 Quick Start

### **1. Open the Template**
```
📁 ViroDB_Import_Template.xlsx
```

### **2. Fill Your Data**
The template contains **8 sheets** (one per table):

| Sheet | Purpose | Key Fields |
|-------|---------|------------|
| **hosts** | Animal host data | scientific_name, sex, weight_g |
| **locations** | Geographic data | province, district, village |
| **samples** | Sample records | sample_code, collection_date |
| **screening** | Test results | test_type, test_result, ct_value |
| **storage** | Storage info | freezer_name, cabinet_no, box_no |
| **taxonomy** | Species classification | scientific_name, genus, family |
| **sample_summary** | Sample overview | total_sequences, virus_types |
| **sample_viruses** | Virus links | sample_id, virus_type |

### **3. Import Your Data**
1. Go to **Automatic Excel Import** in the web interface
2. Upload your filled Excel file
3. Review automatic column mapping
4. Confirm import

---

## 📋 Example Data Structure

### **🏥 Hosts Sheet Example**
| scientific_name | common_name | sex | age_class | weight_g |
|-----------------|-------------|-----|-----------|----------|
| *Rousettus leschenaultii* | Leschenault's rousette | Male | Adult | 45.5 |
| *Pteropus vampyrus* | Large flying fox | Female | Adult | 120.0 |

### **🧪 Samples Sheet Example**
| sample_code | sample_type | collection_date | storage_temperature |
|-------------|-------------|-----------------|-------------------|
| BAT001-2023-01-15 | Tissue | 2023-01-15 | -20°C |
| BAT002-2023-01-16 | Blood | 2023-01-16 | -80°C |

### **🔬 Screening Sheet Example**
| sample_id | test_type | test_result | ct_value |
|-----------|-----------|-------------|----------|
| 1 | qRT-PCR | Positive | 28.5 |
| 2 | qRT-PCR | Negative | 35.2 |

---

## ⚠️ Important Rules

### **✅ Required Fields**
- `samples.sample_code` - Must be unique
- `screening.sample_id` - Must reference existing sample
- `storage.sample_id` - Must reference existing sample
- `taxonomy.scientific_name` - Must be unique
- `sample_viruses.sample_id` - Must reference existing sample
- `sample_viruses.virus_type` - Must be specified

### **📅 Date Formats**
- **Date**: YYYY-MM-DD (e.g., 2023-01-15)
- **Date/Time**: YYYY-MM-DD HH:MM:SS (e.g., 2023-01-25 14:30:00)

### **🔗 Relationships**
```
samples → screening (via sample_id)
samples → storage (via sample_id)
locations → samples (via location_id)
taxonomy → hosts (via scientific_name)
```

---

## 🚫 Excluded Tables

These tables are **NOT** available for Excel import:
- `sequences` - Managed by sequence analyzer
- `consensus_sequences` - Managed by sequence analyzer
- `blast_results` - Managed by BLAST analyzer
- `blast_hits` - Managed by BLAST analyzer
- `projects` - Manual creation only
- `security_*` tables - Managed by security system
- `RecycleBin` - System managed

---

## 🎉 Ready to Import!

Your Excel import template is ready with:
- ✅ **8 available tables** for data entry
- ✅ **Sample data** included as examples
- ✅ **Proper formatting** and column structure
- ✅ **Auto-adjusted column widths** for readability

**📥 Start importing your data now!** 🚀

---

## 📞 Need Help?

- 📖 **Detailed Guide**: `EXCEL_IMPORT_TEMPLATE_GUIDE.md`
- 🛠️ **Table Management**: `manage_excel_exclusions.py`
- 🔧 **Import System**: `database/excel_import.py`
