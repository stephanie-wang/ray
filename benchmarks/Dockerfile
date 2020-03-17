FROM continuumio/anaconda3

# Uncomment these if you want to build ray from source.
RUN apt-get -qq update
run apt-get install -y build-essential curl unzip wget

# Install Anaconda.
# RUN wget --quiet https://repo.continuum.io/archive/Anaconda3-5.0.1-Linux-x86_64.sh
# RUN bash Anaconda3-5.0.1-Linux-x86_64.sh -b -p $HOME/anaconda3
# RUN echo 'export PATH="$HOME/anaconda3/bin:$PATH"' >> ~/.bashrc
# RUN source ~/.bashrc

# Install nodejs.
RUN curl -sL https://deb.nodesource.com/setup_10.x | bash -
RUN apt install nodejs

# Build Ray.
RUN pip install boto3==1.4.8 cython==0.29.0
RUN git clone https://github.com/edoakes/ray
RUN ray/ci/travis/install-bazel.sh
RUN cd ray/python/ray/dashboard/client && npm ci && npm run build
RUN cd ray/python; git fetch; git checkout centralized-experiment; git pull; pip install -e . --verbose

ADD *.py /
