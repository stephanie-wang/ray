# Make sure the SSH session has the correct version of Python on its path.
# You will probably have to change the line below.
export PATH=/home/ubuntu/anaconda3/bin/:$PATH
# Do pushd/popd to make sure we end up in the same directory.
pushd .
# Upgrade Ray.
cd ray
git fetch origin
git checkout xray-task-reconstruction
git reset --hard origin/xray-task-reconstruction
#git checkout fault-tolerance-tests
#git checkout reconstruction-suppression-tests

#rm -rf src/thirdparty/arrow
#cd python
#python setup.py develop

cd python/ray/core
make

popd
