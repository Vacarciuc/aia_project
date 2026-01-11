from src.command import Command, CommandEnum, RequestParams
from src.preview_data import PreviewData
from src.graph_enum import GraphType
from src.model_command import ModelCommand, ModelType


def main() -> None:

    request_params = RequestParams()
    request_params.latitude = 47.0269
    request_params.longitude = 28.8416
    request_params.start_date = "2020-01-01"
    request_params.end_date = "2026-01-01"

    command = Command()
    list_columns = ['date', 'temperature_2m']
    # df_dirty = command.execute(CommandEnum.API_REQUEST, request_params)
    # print(df_dirty.head())
    # d_dirty = PreviewData(df_dirty)
    # d_dirty.draw(GraphType.SCATTER, list_columns)
    # d_dirty.draw(GraphType.HISTOGRAM, columns=['temperature_2m'])
    # print("Command API request executed with success!")
    #
    # df_clean = command.execute(CommandEnum.SAVE_CLEAN_DATA, request_params)
    # print(df_clean.head())
    # d_clean = PreviewData(df_clean)
    # d_clean.draw(GraphType.SCATTER, list_columns)
    # d_clean.draw(GraphType.HISTOGRAM, columns=['temperature_2m'])
    # print("Command clean data executed with success!")

    df_analyze = command.execute(CommandEnum.ANALYZE_DATA, request_params)
    print(df_analyze.head())
    d_analyze = PreviewData(df_analyze)
    d_analyze.draw(GraphType.SCATTER, list_columns)
    d_analyze.draw(GraphType.HISTOGRAM, columns=['temperature_2m'])
    print("Command analyze data executed with success!")

    model_command = ModelCommand(df_analyze)
    model_command.execute(model_type=ModelType.RANDOM_FOREST_REGRESSOR)
    model_command.execute(model_type=ModelType.GRADIENT_BOOSTING_REGRESSOR)

    print('Done!')


if __name__ == '__main__':
    main()


