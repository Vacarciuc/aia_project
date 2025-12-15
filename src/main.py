import WeatherDataLoader as WDL


def main():
    path = "C:/Users/enigma/Desktop/AIA/data sets"

    loader = WDL.WeatherDataLoader(
        root_path=path,
        file_names=[
            '72529014768',
        ]
    )

    data = loader.load_files_from_all_folders()

    print(data)


if __name__ == '__main__':
    main()