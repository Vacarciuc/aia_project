from src.command import Command, CommandEnum, RequestParams
from src.preview_data import PreviewData
from src.graph_enum import GraphType
from src.model_command import ModelCommand, ModelType


def main() -> None:

    request_params = RequestParams()
    request_params.latitude = 47.0269
    request_params.longitude = 28.8416
    request_params.start_date = "2024-01-01"
    request_params.end_date = "2025-12-31"

    command = Command()
    df_dirty = command.execute(CommandEnum.API_REQUEST, request_params)
    print(df_dirty.head())
    print("Command API request executed with success!")

    df_clean = command.execute(CommandEnum.SAVE_CLEAN_DATA, request_params)
    print(df_clean.head())
    print("Command clean data executed with success!")

    df_analyze = command.execute(CommandEnum.ANALYZE_DATA, request_params)
    print(df_analyze.head())
    print("Command analyze data executed with success!")


    print('Done!')


if __name__ == '__main__':
    main()


