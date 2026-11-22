
from ipykernel.kernelapp import IPKernelApp
from .kernel import ILegendRouterKernel

IPKernelApp.launch_instance(kernel_class=ILegendRouterKernel)
