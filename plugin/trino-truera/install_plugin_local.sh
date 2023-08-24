mvn clean install -Dair.check.skip-all=true -DskipTests
if [ -z "${AILENS_DEPENDENCIES_DIR_OVERRIDE}" ]
then
    echo "Expected AILENS_DEPENDENCIES_DIR_OVERRIDE."
    exit 1
fi
rm -rf $AILENS_DEPENDENCIES_DIR_OVERRIDE/trino-server/plugin/trino-truera*
unzip target/trino-truera-*.zip -d $AILENS_DEPENDENCIES_DIR_OVERRIDE/trino-server/plugin

echo "Installed Plugin at time:"$(date)
