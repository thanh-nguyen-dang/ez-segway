cd data/i2/random/1000
touch lines; for f in `ls flows*`; do wc -l $f >> lines; done
cat lines | cut -f1 -d ' ' | sort -u | head -n1 | awk '{ print ($1 - 1)}'
cat lines | cut -f1 -d ' ' | sort -u | tail -n1 | awk '{ print ($1 - 1)}'
rm lines

