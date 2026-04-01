
function test_variable() {
    n=17
}

for n in {1..4}; do
    echo "before n: $n"
    test_variable
    echo "after n: $n"
done