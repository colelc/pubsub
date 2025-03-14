#!/bin/sh

set -e
set -x
urlencoded_dn=$(jq --null-input --raw-output --args '$ARGS.positional[0] | @uri' "$1")
#echo " urlencoded_dn is " $urlencoded_dn

url="https://go.fuqua.duke.edu/fuqua_link/rest/ldap/groupdn/$urlencoded_dn"
list_name=$(\
    curl --max-time 10 --silent --fail $url \
    | jq --raw-output 'if .success then .group.mail else empty end | rtrimstr("@fuqua.duke.edu")' \
    | tr '[:upper:]' '[:lower:]' \
)
# list_name=$(\
# 	curl --max-time 10 --silent --fail "https://go.fuqua.duke.edu/fuqua_link/rest/ldap/groupdn/$urlencoded_dn" \
# 	| jq --raw-output 'if .success then .group.mail else empty end | rtrimstr("@fuqua.duke.edu")' \
# 	| tr '[:upper:]' '[:lower:]' \
# )

echo "list name is" $list_name

#echo "Number of arguments is $#"
#echo "All arguments: $@"
echo "dn:" $1
work_dir=$2
echo "work_dir:" $work_dir
staging_dir=$3
echo "staging dir:" $staging_dir


#echo "ARGS.positional[0] is" $ARGS.positional[0]
# urlencoded_dn=$(jq --null-input --raw-output --args '$ARGS.positional[0] | @uri' "$1")
#urlencoded_dn=$(jq -sRr '@uri' <<< $1)
#echo "urlencoded_dn:" $urlencoded_dn
#dn=$1
#echo "dn: " $dn
#url="https://go.fuqua.duke.edu/fuqua_link/rest/ldap/groupdn/"$dn
#encoded_url=$(printf %s $url  |  jq -sRr '@uri')
#echo "encoded_url: " $encoded_url
#url="https://go.fuqua.duke.edu/fuqua_link/rest/ldap/groupdn/$dn"
#echo "url: " $url

# list_name=$(curl --max-time 10 --silent --fail "https://go.fuqua.duke.edu/fuqua_link/rest/ldap/groupdn/$urlencoded_dn" | jq --raw-output 'if .success then .group.mail else empty end | rtrimstr("@fuqua.duke.edu")' | tr '[:upper:]' '[:lower:]')
# list_name=$(curl --max-time 10 --silent --fail "https://go.fuqua.duke.edu/fuqua_link/rest/ldap/groupdn/$urlencoded_dn")
#list_name=$( \
#  curl --max-time 10 --silent --fail $url \
#  | jq --raw-output 'if .success then .group.mail else empty end | rtrimstr("@fuqua.duke.edu")' \
#  |  tr '[:upper:]' '[:lower:]' \
#)


#echo "list_name:" $list_name


# rebuild_list() {
#     out="/tmp/job-${list_name}-rebuild.out"
#     python3 "$HOME/publish_list.py" --debug --output "$2" "$1" > "$out"
#     [ -f /tmp/debug ] || rm "$out"
# }

# install_list() {
#     out="/tmp/job-${list_name}-install-${1}.out"
#     ssh "$1" "sudo /usr/local/bin/fuqua/dist_list_install_from_nfs.sh" > "$out"
#     [ -f /tmp/debug ] || rm "$out"
# }

#work_dir=/var/tmp/dist_lists
current_dir=$(pwd)
echo "current_dir:" $current_dir
if [[ "$current_dir" == "$work_dir" ]]; then
    echo "Oh no: the current dir is the working dir.  This is not allowed."
    exit
fi

echo "Removing directory:" $work_dir
#rm -fr /var/tmp/dist_lists
rm -rf $work_dir

echo "Re-creating directory:" $work_dir
#mkdir /var/tmp/dist_lists
mkdir $work_dir

echo "Rebuilding list: rebuild_list" $1 $work_dir
# rebuild_list "$1" /var/tmp/dist_lists
#rebuild_list "$1" "$work_dir"

echo "Cleaning out staging directory:" $staging_dir
# rm -v -f \
#     "$staging_dir/${list_name}" \
#     "$staging_dirs/${list_name}.aliases" \
#     "$staging_dir/${list_name}.authusers" \
#     "$staging_dir/${list_name}.config" \
#     "$staging_dir/${list_name}.passwd"
# rm -v -f \
#     "/mnt/dist_lists/${list_name}" \
#     "/mnt/dist_lists/${list_name}.aliases" \
#     "/mnt/dist_lists/${list_name}.authusers" \
#     "/mnt/dist_lists/${list_name}.config" \
#     "/mnt/dist_lists/${list_name}.passwd"

echo "Moving work files into staging directory"
echo "find" $work_dir "-mindepth 1 -print0 | xargs -0 -r -I{} mv -v {}" $staging_dir
# find $work_dir -mindepth 1 -print0 | xargs -0 -r -I{} mv -v {} $staging_dir
# find /var/tmp/dist_lists -mindepth 1 -print0 | xargs -0 -r -I{} mv -v {} /mnt/dist_lists

echo "This would be where we update the SMTP servers in parallel"
# Update the SMTP servers in parallel
# for host in $(awk '/^Host ([a-z-]+)$/ { print $2 }' "$HOME/.ssh/config") ; do
#     install_list "$host" &
# done
#wait

echo "This is where we test to confirm that the list has successfully been published"
# Indicate that the list has been successfully published
#exec curl --max-time 10 --silent --fail -XPUT "https://go.fuqua.duke.edu/fuqua_link/rest/ldap/publishlist/$urlencoded_dn"
