from src.command import Command, CommandEnum, RequestParams
from src.preview_data import PreviewData
from src.graph_enum import GraphType
from src.model_command import ModelCommand, ModelType
from src.power_plant_service import PowerPLantService


def main() -> None:
    serial_number = 84071569

    command = Command()
    df_initial = command.execute(CommandEnum.POWER_PLANT, serial_number)
    print(df_initial.head())
    print('Command Power Plant execute with success!')

    df_dirty = command.execute(CommandEnum.API_REQUEST, serial_number)
    print(df_dirty.head())
    print("Command API request executed with success!")

    df_join = command.execute(CommandEnum.JOIN_DATA, serial_number)
    print(df_join.head())
    print("Command Join executed with success!")

    df_clean = command.execute(CommandEnum.SAVE_CLEAN_DATA, serial_number)
    print(df_clean.head())
    print("Command clean data executed with success!")

    df_analyze = command.execute(CommandEnum.ANALYZE_DATA, serial_number)
    print(df_analyze.head())
    print("Command analyze data executed with success!")


    print('Done!')


if __name__ == '__main__':
    main()


