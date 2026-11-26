FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY JobEngine.slnx ./
COPY src/JobEngine/JobEngine.csproj src/JobEngine/
COPY src/JobEngine.Demo/JobEngine.Demo.csproj src/JobEngine.Demo/
COPY tests/JobEngine.Tests/JobEngine.Tests.csproj tests/JobEngine.Tests/
RUN dotnet restore JobEngine.slnx

COPY . .
RUN dotnet publish src/JobEngine.Demo/JobEngine.Demo.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final
WORKDIR /app
COPY --from=build /app/publish .

ENV ASPNETCORE_HTTP_PORTS=8080
EXPOSE 8080
USER $APP_UID
ENTRYPOINT ["dotnet", "JobEngine.Demo.dll"]
