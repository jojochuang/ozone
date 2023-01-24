        #!/bin/bash
        # Reference: http://stackoverflow.com/questions/59895/can-a-bash-script-tell-what-directory-its-stored-in
            SOURCE="${BASH_SOURCE[0]}"
            SCRIPTS_DIR="$( dirname "$SOURCE" )"
            while [ -h "$SOURCE" ]
            do
                SOURCE="$(readlink "$SOURCE")"
                [[ $SOURCE != /* ]] && SOURCE="$SCRIPTS_DIR/$SOURCE"
                SCRIPTS_DIR="$( cd -P "$( dirname "$SOURCE"  )" && pwd )"
            done
            SCRIPTS_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
            LIB_DIR=$SCRIPTS_DIR/../lib
            TEMP_DIR=$SCRIPTS_DIR/temp


        function untar_file
        {
          if [ -d "$TEMP_DIR" ]; then rm -rf $TEMP_DIR; fi
          mkdir $TEMP_DIR
          cd $TEMP_DIR && hdfs dfs -get /user/tez/0.9.1.7.1.8.3-*/tez.tar.gz ./
          cd $TEMP_DIR && tar -zxf tez.tar.gz
          rm $TEMP_DIR/tez.tar.gz
        }

        function tar_file
        {
          if [ -f $SCRIPTS_DIR/tez.tar.gz ]; then rm $SCRIPTS_DIR/tez.tar.gz; fi
          cd $TEMP_DIR && tar -czf ../tez.tar.gz *
          hdfs dfs -rm /user/tez/0.9.1.7.1.8.3-*/tez.tar.gz
          cd $SCRIPTS_DIR && hdfs dfs -copyFromLocal tez.tar.gz /user/tez/0.9.1.7.1.8.3-*/
          rm -rf $TEMP_DIR
          rm $SCRIPTS_DIR/tez.tar.gz
        }

        function replace_tar
        {
          untar_file
          if [ -f $TEMP_DIR/lib/ozone-filesystem-hadoop3-*.jar ]; then rm $TEMP_DIR/lib/ozone-filesystem-hadoop3-*.jar; fi
          cp $LIB_DIR/hadoop-ozone/share/ozone/lib/ozone-filesystem-hadoop3-1.3.0.718*.jar $TEMP_DIR/lib
          tar_file
        }
         
        function validate_tar
        {
          untar_file
          if [ -f $TEMP_DIR/lib/ozone-filesystem-hadoop3-1.3.0.718*.jar ]; then
            echo "Validation SUCCESSFUL"
          else
            echo "Validation FAILED, ozone-filesystem-hadoop3 jar from the Ozone parcel is not present"
          fi
          rm -rf $TEMP_DIR
        }

        function restore_tar
        {
          untar_file
          if [ -f $TEMP_DIR/lib/ozone-filesystem-hadoop3-*.jar ]; then rm $TEMP_DIR/lib/ozone-filesystem-hadoop3-*.jar; fi
          cp "$1"/lib/hadoop-ozone/share/ozone/lib/ozone-filesystem-hadoop3-1.2.0.7.1.8.3*.jar $TEMP_DIR/lib
          tar_file
        }

       function usage
       {
         description=$'Description:\n\n--replace flag replaces the Tez client jar with Ozone parcel FS jar\n--validate flag verifies if the Ozone parcel FS jar has been replaced correctly\n--restore flag replaces the Tez client jar with the CDH parcel Ozone FS jar\n\n'
         usage_text=$'Usage text:\n\nbash update_tez_client_jar.sh --replace|--validate|--restore (CDH parcel path)'
         echo "${description}${usage_text}"
         exit 1
       }

       while true; do
         case $1 in
           --replace)
             replace_tar
             break
           ;;
           --validate)
             validate_tar
             break
           ;;
           --restore)
             shift
             if [[ -n "$1" ]]; then
               restore_tar "$1"
             else
               usage
             fi
             break
           ;;
           *)
             usage
           ;;
         esac
       done
