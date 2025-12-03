"""
FDI_0030_registShapefileToWork.py

処理名:
    データ登録

概要:
    データ標準化後ストレージ(S3)の標準仕様3Dシェープファイルを読み込み、一時DBに登録する。
    ・対象の設備データについて取込管理テーブルにレコードを追加する。
    ・一時DBへのテーブル作成と標準化仕様3Dシェープファイルの設備データの取込を行う。
    ・一時DBへの登録が完了した標準仕様3Dシェープファイルをデータ標準化後ストレージ(S3)の取込後ディレクトリに移動させる。
    ・データ登録完了時、取込管理テーブルの一時テーブル取込終了日時カラムを現在日時で更新する。

実行コマンド形式:
    python3 [バッチ格納先パス]/FDI_0030_registShapefileToWork.py
    --shapefile_name=[標準仕様3Dシェープファイル名] --provider_id=[公益事業者・道路管理者ID]
"""

import argparse
import os
import shutil
import subprocess
import traceback
from datetime import datetime
from pathlib import Path

import boto3

from core.config_reader import read_config
from core.database import Database
from core.logger import LogManager
from core.message import get_message
from core.validations import Validations
from core.secretProperties import SecretPropertiesSingleton
from util.updateImportManagement import update_import_management

log_manager = LogManager()
logger = log_manager.get_logger("FDI_0030_データ登録")
config = read_config(logger)

AWS_REGION = config["aws"]["region"].strip()
SHAPEFILE_DIR_PATH = config["folderPass"]["shapefile_dir_path"].strip()
PRE_IMPORT_SHAPEFILE_DIR_PATH = config["aws"]["pre_import_shapefile_dir_path"].strip()

CODE_LIST = {
    "shapefile_name": "標準仕様3Dシェープファイル名",
    "provider_id": "公益事業者・道路管理者ID",
}


# 起動パラメータを受け取る関数
def parse_args():
    try:
        # 完全一致のみ許可
        parser = argparse.ArgumentParser(allow_abbrev=False, exit_on_error=False)
        parser.add_argument("--shapefile_name", required=False)
        parser.add_argument("--provider_id", required=False)
        return parser.parse_args()
    except Exception as e:
        # コマンドライン引数の解析に失敗した場合
        logger.error("BPE0037", str(e.message))
        logger.process_error_end()


# 1.入力値チェック
def validate_inputs(params):

    # パラメータを定義
    check_params = list(CODE_LIST.keys())

    # 必須パラメータが不足している場合のチェック
    for value, key in zip(params, check_params):
        if not value:
            logger.error("BPE0018", CODE_LIST[key])
            logger.process_error_end()

    # パラメータを展開
    (
        shapefile_name,
        provider_id,
    ) = params

    # ファイル名の拡張子は「.zip」であるか
    if not Validations.is_suffix(shapefile_name, ".zip"):
        logger.error("BPE0019", "標準仕様3Dシェープファイル名", shapefile_name)
        logger.process_error_end()

    # ファイル名の先頭が「shape_3d_」であるか
    if not shapefile_name.startswith("shape_3d_"):
        logger.error("BPE0019", "標準仕様3Dシェープファイル名", shapefile_name)
        logger.process_error_end()

    # 公益事業者・道路管理者ID：1以上9223372036854775807以下の整数であるか
    if not provider_id.isdigit() or not (1 <= int(provider_id) <= 9223372036854775807):
        logger.error("BPE0019", CODE_LIST["provider_id"], provider_id)
        logger.process_error_end()

    return params


# 2.設備小項目等取得
def get_fac_subitem(conn, shapefile_name, provider_id, db_mst_schema):
    fac_subitem_eng = f"{shapefile_name.split('_')[2]}_{shapefile_name.split('_')[3]}"

    # 2-1. 公益事業者・道路管理者マスタとの整合性チェック
    query = (
        f"SELECT EXISTS (SELECT 1 FROM {db_mst_schema}.mst_provider "
        "WHERE provider_id = (%s))"
    )
    result = Database.execute_query(conn, logger, query, (provider_id,), fetchone=True)
    if not result:
        logger.error("BPE0003", "公益事業者・道路管理者ID", provider_id)
        logger.process_error_end()

    # 2-2. ファイル名との整合性チェック
    provider_code = shapefile_name.split('_')[4]
    provider_name = shapefile_name.split('_')[5]
    query = (
        f"SELECT provider_id FROM {db_mst_schema}.mst_provider "
        "WHERE provider_code = (%s) AND provider_name = (%s)"
    )
    result = Database.execute_query(conn, logger, query, (provider_code, provider_name), fetchone=True)
    if str(result) != provider_id:
        logger.error("BPE0052", shapefile_name, provider_id)
        logger.process_error_end()

    # 2-3. 設備小項目マスタとの整合性チェック
    query = (
        f"SELECT EXISTS (SELECT 1 FROM {db_mst_schema}.mst_fac_subitem "
        "WHERE fac_subitem_eng = (%s))"
    )
    result = Database.execute_query(
        conn, logger, query, (fac_subitem_eng,), fetchone=True
    )
    if not result:
        logger.error("BPE0051", fac_subitem_eng)
        logger.process_error_end()

    # 解凍後シェープファイル名を取得
    unzipped_shapefile_name = shapefile_name.removesuffix(".zip")

    # 一時DBテーブル名を取得
    work_table_name = f"work_{fac_subitem_eng}_{provider_id}"

    # 設備データ管理マスタDBテーブル名を取得
    fac_data_master_table_name = f"data_{fac_subitem_eng}_{provider_id}"

    return (
        fac_subitem_eng,
        unzipped_shapefile_name,
        work_table_name,
        fac_data_master_table_name,
    )


# 3.取込管理テーブルにレコード作成
def insert_mst_import_management(
    conn,
    provider_id,
    fac_subitem_eng,
    shapefile_name,
    work_table_name,
    fac_data_master_table_name,
    db_mst_schema,
):
    try:
        query = f"""
            INSERT INTO {db_mst_schema}.mst_import_management(
                provider_code,
                fac_subitem_id,
                status_code,
                shapefile_name,
                work_table_name,
                work_table_import_start_date,
                fac_data_master_table_name,
                created_by,
                created_at)
            VALUES(
                (SELECT provider_code FROM {db_mst_schema}.mst_provider WHERE provider_id = %s),
                (SELECT fac_subitem_id FROM {db_mst_schema}.mst_fac_subitem WHERE fac_subitem_eng = %s),
                '10',
                %s,
                %s,
                NOW(),
                %s,
                'system',
                NOW())
        """
        Database.execute_query(
            conn,
            logger,
            query,
            (
                provider_id,
                fac_subitem_eng,
                shapefile_name,
                work_table_name,
                fac_data_master_table_name,
            ),
            commit=True,
            raise_exception=True,
        )

        # 取込管理IDを取得
        query2 = f"""
            SELECT import_id FROM {db_mst_schema}.mst_import_management
            WHERE provider_code = (
                SELECT provider_code FROM {db_mst_schema}.mst_provider WHERE provider_id = %s
            )
            AND fac_subitem_id = (
                SELECT fac_subitem_id FROM {db_mst_schema}.mst_fac_subitem WHERE fac_subitem_eng = %s
            )
            ORDER BY created_at DESC
            LIMIT 1
        """
        import_id = Database.execute_query(
            conn,
            logger,
            query2,
            (
                provider_id,
                fac_subitem_eng,
            ),
            fetchone=True,
            commit=True,
        )
        return import_id
    except Exception:
        logger.error("BPE0047", fac_subitem_eng, provider_id)
        logger.process_error_end()


# 4.シェープファイルダウンロード
def download_shapefile(secret_props, shapefile_name, conn, import_id):
    try:
        # S3クライアント作成
        s3 = boto3.client(
            "s3",
            region_name=AWS_REGION,
        )
        # シェープファイルダウンロード
        s3.download_file(
            secret_props.get("standardized_bucket_name"),
            f"{PRE_IMPORT_SHAPEFILE_DIR_PATH}/{shapefile_name}",
            f"{SHAPEFILE_DIR_PATH}/{shapefile_name}",
        )
    except Exception:
        # 取込管理テーブル更新
        update_import_management(
            conn,
            logger,
            import_id,
            "91",
            get_message("BPE0048").format(shapefile_name),
            None,
            None,
            None
        )
        logger.error("BPE0048", shapefile_name)
        logger.process_error_end()


# 5.zipファイル解凍
def unzip_shapefile(shapefile_name, unzipped_shapefile_name, conn, import_id):
    try:
        shutil.unpack_archive(
            f"{SHAPEFILE_DIR_PATH}/{shapefile_name}",
            f"{SHAPEFILE_DIR_PATH}/{unzipped_shapefile_name}",
        )
    except Exception:
        # 取込管理テーブル更新
        update_import_management(
            conn,
            logger,
            import_id,
            "91",
            get_message("BPE0012").format("標準仕様3Dシェープファイル名", shapefile_name),
            None,
            None,
            None
        )
        # 標準仕様3Dシェープファイル削除
        os.remove(f"{SHAPEFILE_DIR_PATH}/{shapefile_name}")
        logger.error("BPE0012", "標準仕様3Dシェープファイル名", shapefile_name)
        logger.process_error_end()


# 6.構成ファイルチェック
def check_file_structure(unzipped_shapefile_name, conn, import_id, shapefile_name):
    unzipped_shapefile_path = Path(f"{SHAPEFILE_DIR_PATH}/{unzipped_shapefile_name}")
    required = [".shp", ".dbf", ".shx", ".prj"]
    missing = []
    for ext in required:
        if not any(unzipped_shapefile_path.glob(f"*{ext}")):
            missing.append(f"*{ext}")
    if missing:
        # 取込管理テーブル更新
        update_import_management(
            conn,
            logger,
            import_id,
            "91",
            get_message("BPE0035").format(", ".join(missing)),
            None,
            None,
            None
        )
        # 標準仕様3Dシェープファイル削除
        os.remove(f"{SHAPEFILE_DIR_PATH}/{shapefile_name}")
        # 解凍後シェープファイル削除
        shutil.rmtree(f"{SHAPEFILE_DIR_PATH}/{unzipped_shapefile_name}")
        logger.error("BPE0035", ", ".join(missing))
        logger.process_error_end()


# 7.DDL・DML作成
def create_ddl_dml(
    fac_subitem_eng,
    provider_id,
    unzipped_shapefile_name,
    work_table_name,
    secret_props,
    conn,
    import_id,
    shapefile_name,
):
    try:
        # 7-1.DDL書き込み
        sql_dir_path = config["folderPass"]["sql_dir_path"].strip()
        db_work_schema = secret_props.get("db_work_schema")
        sql_file_path = f"{sql_dir_path}/{fac_subitem_eng}_{provider_id}.sql"
        # 解凍後シェープファイルの.shpファイル一覧を取得
        unzipped_shapefile_path = f"{SHAPEFILE_DIR_PATH}/{unzipped_shapefile_name}"
        file_list_all = os.listdir(unzipped_shapefile_path)
        file_list = [
            f"{unzipped_shapefile_path}/{file}"
            for file in file_list_all
            if file.endswith(".shp")
        ]
        cmd = [
            "shp2pgsql",
            "-p",
            "-W",
            "UTF-8",
            "-s",
            "4326",
            "-I",
            file_list[0],
            f"{db_work_schema}.{work_table_name}",
        ]
        with open(sql_file_path, "w", encoding="utf-8") as f:
            # コマンド実行
            subprocess.run(cmd, stdout=f, check=True)

        with open(sql_file_path, "r", encoding="utf-8") as f:
            content = f.read()
            # gidカラムと主キー設定を削除
            content = content.replace("gid serial,", "")
            content = content.replace(
                (
                    f'ALTER TABLE "{db_work_schema}"."{work_table_name}" '
                    "ADD PRIMARY KEY (gid);"
                ),
                "",
            )
        with open(sql_file_path, "w", encoding="utf-8") as f:
            f.write(content)

        # 7-2.DML書き込み
        for file in file_list:
            cmd = [
                "shp2pgsql",
                "-a",
                "-D",
                "-W",
                "UTF-8",
                "-s",
                "4326",
                file,
                f"{db_work_schema}.{work_table_name}",
            ]
            with open(sql_file_path, "a", encoding="utf-8") as f:
                # コマンド実行
                subprocess.run(cmd, stdout=f, check=True)
        with open(sql_file_path, "r", encoding="utf-8") as f:
            content = f.read()
            # BEGIN;、COMMIT;を削除
            content = content.replace("BEGIN;", "")
            content = content.replace("COMMIT;", "")
        # BEGIN;、COMMIT;を追加
        new_content = "BEGIN;\n" + content + "\nCOMMIT;\n"
        with open(sql_file_path, "w", encoding="utf-8") as f:
            f.write(new_content)

        return sql_file_path
    except Exception:
        # 取込管理テーブル更新
        update_import_management(
            conn,
            logger,
            import_id,
            "91",
            get_message("BPE0049").format(unzipped_shapefile_name),
            None,
            None,
            None
        )
        # 標準仕様3Dシェープファイル削除
        os.remove(f"{SHAPEFILE_DIR_PATH}/{shapefile_name}")
        # 解凍後シェープファイル削除
        shutil.rmtree(f"{SHAPEFILE_DIR_PATH}/{unzipped_shapefile_name}")
        # SQLファイル削除
        os.remove(sql_file_path)
        logger.error("BPE0049", unzipped_shapefile_name)
        logger.process_error_end()


# 8.一時テーブル作成・データ登録
def create_work_table_and_insert_data(
    secret_props,
    sql_file_path,
    work_table_name,
    conn,
    import_id,
    shapefile_name,
    unzipped_shapefile_name,
):
    try:
        # コマンド作成
        cmd = [
            "psql",
            "-h",
            secret_props.get("db_host"),
            "-p",
            secret_props.get("db_port"),
            "-U",
            secret_props.get("db_user"),
            "-d",
            secret_props.get("db_name"),
            "-v",
            "ON_ERROR_STOP=1",
            "-f",
            sql_file_path,
        ]
        # 環境変数をセット
        env = os.environ.copy()
        env["PGPASSWORD"] = secret_props.get("db_pass")
        # コマンド実行
        subprocess.run(cmd, env=env, capture_output=True, check=True)
    # コマンド実行時の例外処理
    except subprocess.CalledProcessError as e:
        # 取込管理テーブル更新
        update_import_management(
            conn,
            logger,
            import_id,
            "91",
            get_message("BPE0050").format(work_table_name, ""),
            None,
            None,
            None
        )
        # 標準仕様3Dシェープファイル削除
        os.remove(f"{SHAPEFILE_DIR_PATH}/{shapefile_name}")
        # 解凍後シェープファイル削除
        shutil.rmtree(f"{SHAPEFILE_DIR_PATH}/{unzipped_shapefile_name}")
        logger.error("BPE0050", work_table_name, e.stderr.decode().strip())
        logger.process_error_end()


# 9.zipファイル移動
def move_zip_file(secret_props, shapefile_name):
    bool = True
    try:
        # S3クライアント作成
        s3 = boto3.client(
            "s3",
            region_name=AWS_REGION,
        )
        # 取込後シェープファイル格納用ディレクトリパス取得
        imported_shapefile_dir_path = config["aws"][
            "imported_shapefile_dir_path"
        ].strip()
        # シェープファイルコピー
        copy_source = {
            "Bucket": secret_props.get("standardized_bucket_name"),
            "Key": f"{PRE_IMPORT_SHAPEFILE_DIR_PATH}/{shapefile_name}",
        }
        s3.copy_object(
            CopySource=copy_source,
            Bucket=secret_props.get("standardized_bucket_name"),
            Key=f"{imported_shapefile_dir_path}/{shapefile_name}",
        )
        try:
            # シェープファイル削除
            s3.delete_object(
                Bucket=secret_props.get("standardized_bucket_name"),
                Key=f"{PRE_IMPORT_SHAPEFILE_DIR_PATH}/{shapefile_name}",
            )
        # 削除時の例外処理
        except Exception:
            bool = False
            logger.warning("BPW0022", shapefile_name)
    # コピー時の例外処理
    except Exception:
        bool = False
        logger.warning("BPW0020", shapefile_name)
    return bool


# 10.ファイル削除
def delete_local_files(
    shapefile_name, unzipped_shapefile_name, sql_file_path
):
    bool = True
    try:
        # 標準仕様3Dシェープファイル削除
        os.remove(f"{SHAPEFILE_DIR_PATH}/{shapefile_name}")
    except Exception:
        bool = False
        logger.warning("BPW0006", shapefile_name)
    try:
        # 解凍後シェープファイル削除
        shutil.rmtree(f"{SHAPEFILE_DIR_PATH}/{unzipped_shapefile_name}")
    except Exception:
        bool = False
        logger.warning("BPW0006", unzipped_shapefile_name)
    try:
        # SQLファイル削除
        os.remove(sql_file_path)
    except Exception:
        bool = False
        logger.warning("BPW0006", sql_file_path)
    return bool


def main():
    import_id = None
    shapefile_name = None
    unzipped_shapefile_name = None
    sql_file_path = None
    try:
        # 開始ログ出力
        logger.process_start()

        # 起動パラメータの取得
        args = parse_args()

        params = [
            args.shapefile_name,
            args.provider_id,
        ]

        # 1. 共通入力値チェック
        (
            shapefile_name,
            provider_id,
        ) = validate_inputs(params)

        # secret_nameをconfigから取得し、secret_propsにAWS Secrets Managerの値を格納
        secret_name = config["aws"]["secret_name"]
        secret_props = SecretPropertiesSingleton(secret_name, config, logger)

        #シークレットからマスタ管理スキーマ名を取得
        db_mst_schema = secret_props.get("db_mst_schema")

        # DB接続を取得
        conn = Database.get_mstdb_connection(logger)

        # 2.設備小項目等取得
        (
            fac_subitem_eng,
            unzipped_shapefile_name,
            work_table_name,
            fac_data_master_table_name,
        ) = get_fac_subitem(conn, shapefile_name, provider_id, db_mst_schema)

        # 3.取込管理テーブルにレコード作成
        import_id = insert_mst_import_management(
            conn,
            provider_id,
            fac_subitem_eng,
            shapefile_name,
            work_table_name,
            fac_data_master_table_name,
            db_mst_schema,
        )

        # 4.シェープファイルダウンロード
        # download_shapefile(secret_props, shapefile_name, conn, import_id)

        # 5.zipファイル解凍
        unzip_shapefile(shapefile_name, unzipped_shapefile_name, conn, import_id)

        # 6.構成ファイルチェック
        check_file_structure(unzipped_shapefile_name, conn, import_id, shapefile_name)

        # 7.DDL・DML作成
        sql_file_path = create_ddl_dml(
            fac_subitem_eng,
            provider_id,
            unzipped_shapefile_name,
            work_table_name,
            secret_props,
            conn,
            import_id,
            shapefile_name,
        )

        # 8.一時テーブル作成・データ登録
        create_work_table_and_insert_data(
            secret_props, sql_file_path, work_table_name, conn, import_id, shapefile_name, unzipped_shapefile_name
        )

        warn = False

        # 9.zipファイル移動
        # if not move_zip_file(secret_props, shapefile_name, import_id):
        #     warn = True

        # 10.ファイル削除
        if not delete_local_files(
            shapefile_name, unzipped_shapefile_name, sql_file_path
        ):
            warn = True
        
        # 11.取込管理テーブル更新
        update_import_management(
            conn,
            logger,
            import_id,
            None,
            None,
            datetime.now(),
            None,
            None
        )

        # 12. 終了コード返却
        # 終了コード（正常終了）を呼び出し元に返却して、処理を終了する。
        if not warn:
            logger.process_normal_end()
        # 終了コード（警告終了）を呼び出し元に返却して、処理を終了する。
        else:
            logger.process_warning_end()

    except Exception:
        # 取込管理テーブル更新
        if import_id:
            update_import_management(
                conn,
                logger,
                import_id,
                "91",
                get_message("BPE0009").format(""),
                None,
                None,
                None
            )
        # 標準仕様3Dシェープファイル削除
        if shapefile_name and os.path.exists(f"{SHAPEFILE_DIR_PATH}/{shapefile_name}"):
            os.remove(f"{SHAPEFILE_DIR_PATH}/{shapefile_name}")
        # 解凍後シェープファイル削除
        if unzipped_shapefile_name and os.path.exists(
            f"{SHAPEFILE_DIR_PATH}/{unzipped_shapefile_name}"
        ):
            shutil.rmtree(f"{SHAPEFILE_DIR_PATH}/{unzipped_shapefile_name}")
        # SQLファイル削除
        if sql_file_path and os.path.exists(sql_file_path):
            os.remove(sql_file_path)
        logger.error("BPE0009", traceback.format_exc())
        logger.process_error_end()


if __name__ == "__main__":
    main()
