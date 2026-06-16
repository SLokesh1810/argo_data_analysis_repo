from dataset_extraction_ftp import check_new_data
from argo_pipeline import BGC_DIR, NORMAL_DIR

if __name__ == '__main__':
    count, floats = check_new_data(BGC_DIR, NORMAL_DIR)

    print(f"Changes : {count}, floats: {floats}")